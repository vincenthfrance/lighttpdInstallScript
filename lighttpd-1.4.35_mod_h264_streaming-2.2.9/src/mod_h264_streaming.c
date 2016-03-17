/*******************************************************************************
 mod_h264_streaming.c

 mod_h264_streaming - A lighttpd plugin for pseudo-streaming Quicktime/MPEG4 files.

 Copyright (C) 2007-2009 CodeShop B.V.

 Licensing
 The Streaming Module is licened under a Creative Commons License. It
 allows you to use, modify and redistribute the module, but only for
 *noncommercial* purposes. For corporate use, please apply for a
 commercial license.

 Creative Commons License:
 http://creativecommons.org/licenses/by-nc-sa/3.0/

 Commercial License for H264 Streaming Module:
 http://h264.code-shop.com/trac/wiki/Mod-H264-Streaming-License-Version2

 Commercial License for Smooth Streaming Module:
 http://smoothstreaming.code-shop.com/trac/wiki/Mod-Smooth-Streaming-License
******************************************************************************/ 

// Example configuration 

/*
# Add the module to your server list in lighttpd.conf
server.modules = (
  "mod_expire",
  "mod_secdownload",
  "mod_h264_streaming",
  etc...
}

# Files ending in .mp4 and .f4v are served by the module 
h264-streaming.extensions = ( ".mp4", ".f4v" )
# The number of seconds after which the bandwidth is shaped (defaults to 0=disable)
h264-streaming.buffer-seconds = 10

# Add Expires/Cache-Control header
$HTTP["url"] =~ "\.(mp4|f4v)$" {
  expire.url = ( "" => "access 8 hours" )
}

*/

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "base.h"
#include "log.h"
#include "buffer.h"
#include "response.h"
#include "http_chunk.h"
#include "stat_cache.h"
#include "etag.h"

#include "plugin.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mp4_io.h"
#include "mp4_process.h"
#include "moov.h"
#include "output_bucket.h"
#ifdef BUILDING_H264_STREAMING
#include "output_mp4.h"
#define X_MOD_STREAMING_KEY X_MOD_H264_STREAMING_KEY
#define X_MOD_STREAMING_VERSION X_MOD_H264_STREAMING_VERSION
#define H264_STREAMING_HANDLER "h264-streaming.extensions"
#endif
#ifdef BUILDING_SMOOTH_STREAMING
#include "ism_reader.h"
#include "output_ismv.h"
#define X_MOD_STREAMING_KEY X_MOD_SMOOTH_STREAMING_KEY
#define X_MOD_STREAMING_VERSION X_MOD_SMOOTH_STREAMING_VERSION
#define H264_STREAMING_HANDLER "smooth-streaming.extensions"
#endif
#ifdef BUILDING_FLV_STREAMING
#define H264_STREAMING_HANDLER "flv-streaming.extensions"
#include "output_flv.h"
#endif

/* plugin config for all request/connections */

typedef struct {
  array *extensions;
  unsigned short buffer_seconds;
} plugin_config;

typedef struct {
  PLUGIN_DATA;

  plugin_config **config_storage;

  plugin_config conf;
} plugin_data;

typedef struct {
  struct mp4_split_options_t* options;
  int play_position; /* (in seconds) */
  unsigned short buffer_seconds;
} handler_ctx;

static handler_ctx * handler_ctx_init(unsigned short buffer_seconds) {
  handler_ctx * hctx;

  hctx = calloc(1, sizeof(*hctx));

  hctx->options = mp4_split_options_init();
  hctx->play_position = 0;
  hctx->buffer_seconds = buffer_seconds;

  return hctx;
}

static void handler_ctx_free(handler_ctx *hctx) {
  mp4_split_options_exit(hctx->options);
  free(hctx);
}

/* init the plugin data */
INIT_FUNC(mod_h264_streaming_init) {
  plugin_data *p;

  p = calloc(1, sizeof(*p));

  return p;
}

/* detroy the plugin data */
FREE_FUNC(mod_h264_streaming_free) {
  plugin_data *p = p_d;

  UNUSED(srv);

  if (!p) return HANDLER_GO_ON;

  if (p->config_storage) {
    size_t i;

    for (i = 0; i < srv->config_context->used; i++) {
      plugin_config *s = p->config_storage[i];

      if (!s) continue;

      array_free(s->extensions);

      free(s);
    }
    free(p->config_storage);
  }

  free(p);

  return HANDLER_GO_ON;
}

/* handle plugin config and check values */

#define CONFIG_H264STREAMING_EXTENSIONS     "h264-streaming.extensions"
#define CONFIG_H264STREAMING_BUFFER_SECONDS "h264-streaming.buffer-seconds"

SETDEFAULTS_FUNC(mod_h264_streaming_set_defaults) {
  plugin_data *p = p_d;
  size_t i = 0;

  config_values_t cv[] = {
    { CONFIG_H264STREAMING_EXTENSIONS,     NULL, T_CONFIG_ARRAY, T_CONFIG_SCOPE_CONNECTION }, /* 0 */
    { CONFIG_H264STREAMING_BUFFER_SECONDS, NULL, T_CONFIG_SHORT, T_CONFIG_SCOPE_CONNECTION }, /* 1 */
    { NULL,                                NULL, T_CONFIG_UNSET, T_CONFIG_SCOPE_UNSET }
  };

  if (!p) return HANDLER_ERROR;

  p->config_storage = calloc(1, srv->config_context->used * sizeof(specific_config *));

  for (i = 0; i < srv->config_context->used; i++) {
    plugin_config *s;

    s = calloc(1, sizeof(plugin_config));
    s->extensions     = array_init();
    s->buffer_seconds = 0;

    cv[0].destination = s->extensions;
    cv[1].destination = &(s->buffer_seconds);

    p->config_storage[i] = s;

    if (0 != config_insert_values_global(srv, ((data_config *)srv->config_context->data[i])->value, cv)) {
      return HANDLER_ERROR;
    }
  }

  return HANDLER_GO_ON;
}

#define PATCH_OPTION(x) \
  p->conf.x = s->x;
static int mod_h264_streaming_patch_connection(server *srv, connection *con, plugin_data *p) {
  size_t i, j;
  plugin_config *s = p->config_storage[0];

  PATCH_OPTION(extensions);
  PATCH_OPTION(buffer_seconds);

  /* skip the first, the global context */
  for (i = 1; i < srv->config_context->used; i++) {
    data_config *dc = (data_config *)srv->config_context->data[i];
    s = p->config_storage[i];

    /* condition didn't match */
    if (!config_check_cond(srv, con, dc)) continue;

    /* merge config */
    for (j = 0; j < dc->value->used; j++) {
      data_unset *du = dc->value->data[j];

      if (buffer_is_equal_string(du->key, CONST_STR_LEN(CONFIG_H264STREAMING_EXTENSIONS))) {
        PATCH_OPTION(extensions);
      } else
      if (buffer_is_equal_string(du->key, CONST_STR_LEN(CONFIG_H264STREAMING_BUFFER_SECONDS))) {
        PATCH_OPTION(buffer_seconds);
      }
    }
  }

  return 0;
}
#undef PATCH_OPTION

URIHANDLER_FUNC(mod_h264_streaming_path_handler) {
  plugin_data *p = p_d;
  int s_len;
  size_t k;
  handler_ctx *hctx;

  UNUSED(srv);

  if (buffer_is_empty(con->physical.path)) return HANDLER_GO_ON;

  mod_h264_streaming_patch_connection(srv, con, p);

  if (con->plugin_ctx[p->id]) {
    hctx = con->plugin_ctx[p->id];
  } else {
    hctx = handler_ctx_init(p->conf.buffer_seconds);
    con->plugin_ctx[p->id] = hctx;
  }

  s_len = con->physical.path->used - 1;

  for (k = 0; k < p->conf.extensions->used; k++) {
    data_string *ds = (data_string *)p->conf.extensions->data[k];
    int ct_len = ds->value->used - 1;

    if (ct_len > s_len) continue;
    if (ds->value->used == 0) continue;

    if (0 == strncmp(con->physical.path->ptr + s_len - ct_len, ds->value->ptr, ct_len)) {
      buffer *mtime;
      stat_cache_entry *sce = NULL;

      // Range requests are currently not supported, so let mod_staticfile
      // handle it. Obviously the 'start' parameter doesn't work with
      // mod_staticfile and so the movie always plays from the beginning,
      // but at least it makes streaming and seeking in VLC work.
      if (con->conf.range_requests &&
         NULL != array_get_element(con->request.headers, "Range")) {
          return HANDLER_GO_ON;
      }

      response_header_overwrite(srv, con,
        CONST_STR_LEN(X_MOD_STREAMING_KEY),
        CONST_STR_LEN(X_MOD_STREAMING_VERSION));

      if(con->uri.query->ptr && !mp4_split_options_set(hctx->options, con->uri.query->ptr, con->uri.query->used))
      {
        con->http_status = 403;
       	return HANDLER_FINISHED;
      }

      /* get file info */
      if (HANDLER_GO_ON != stat_cache_get_entry(srv, con, con->physical.path, &sce))
      {
        con->http_status = 403;
       	return HANDLER_FINISHED;
      }

      /* we are safe now, let's build a h264 header */
      uint64_t content_length = 0;
      {
        unsigned int filesize = sce->st.st_size;

        struct bucket_t* buckets = 0;
        int verbose = 0;
        int http_status =
          mp4_process(con->physical.path->ptr, filesize, verbose, &buckets, hctx->options);

        if(http_status != 200)
        {
          if(buckets)
          {
            buckets_exit(buckets);
          }

          return http_status;
        }

        response_header_overwrite(srv, con, CONST_STR_LEN("Content-Type"),
                                  CONST_STR_LEN("video/mp4"));

        {
          struct bucket_t* bucket = buckets;
          if(bucket)
          {
            do
            {
              switch(bucket->type_)
              {
              case BUCKET_TYPE_MEMORY:
              {
                buffer* b = chunkqueue_get_append_buffer(con->write_queue);
                buffer_append_memory(b, bucket->buf_, bucket->size_);
                b->used++; /* add virtual \0 */
              }
                break;
              case BUCKET_TYPE_FILE:
                http_chunk_append_file(srv, con, con->physical.path,
                                       bucket->offset_, bucket->size_);
                break;
              }
              content_length += bucket->size_;
              bucket = bucket->next_;
            } while(bucket != buckets);
            buckets_exit(buckets);
          }
        }
      }

      // ETag header
      {
        buffer *etag = buffer_init_buffer(sce->etag);
        buffer_append_off_t(etag, content_length);
        etag_mutate(con->physical.etag, etag);
        response_header_overwrite(srv, con, CONST_STR_LEN("ETag"),
                                  CONST_BUF_LEN(con->physical.etag));
        buffer_free(etag);
      }

      // Last-Modified header
      mtime = strftime_cache_get(srv, sce->st.st_mtime);
      response_header_overwrite(srv, con, CONST_STR_LEN("Last-Modified"),
                                CONST_BUF_LEN(mtime));

      if (HANDLER_FINISHED == http_response_handle_cachable(srv, con, mtime))
      {
        return HANDLER_FINISHED;
      }

      con->file_finished = 1;

      if(hctx->buffer_seconds && hctx->buffer_seconds < hctx->options->seconds)
      {
        off_t moov_offset = hctx->options->byte_offsets[hctx->buffer_seconds];
        con->conf.kbytes_per_second = moov_offset / 1024;
      }

      return HANDLER_FINISHED;
    }
  }

  /* not found */
  return HANDLER_GO_ON;
}

TRIGGER_FUNC(mod_h264_streaming_trigger) {
  plugin_data *p = p_d;
  size_t i;

  /* check all connections */
  for (i = 0; i < srv->conns->used; i++) {
    connection *con = srv->conns->ptr[i];
    handler_ctx *hctx = con->plugin_ctx[p->id];
    int t_diff = srv->cur_ts - con->connection_start;

    if(hctx == NULL)
      continue;

    // check if bandwidth shaping is enabled
    if(!hctx->buffer_seconds)
    {
      continue;
    }

    hctx->play_position = srv->cur_ts - con->connection_start;

    // when we are near the end you're no longer throttled
    if((t_diff + hctx->buffer_seconds) >= hctx->options->seconds)
    {
      con->conf.kbytes_per_second = 0;
      continue;
    }

    off_t moov_offset = hctx->options->byte_offsets[t_diff + hctx->buffer_seconds];

    if(con->bytes_written < moov_offset)
    {
      con->conf.kbytes_per_second = 1 + (moov_offset - con->bytes_written) / 1024;
    }
    else
    {
      // throttle connection to a low speed (set to the size of the TCP send buffer) as the buffer is full
      con->conf.kbytes_per_second = 32;
    }
  }

  return HANDLER_GO_ON;
}

handler_t mod_h264_streaming_cleanup(server *srv, connection *con, void *p_d) {
  plugin_data *p = p_d;
  handler_ctx *hctx = con->plugin_ctx[p->id];

  UNUSED(srv);

  if(hctx == NULL)
    return HANDLER_GO_ON;

  handler_ctx_free(hctx);
  con->plugin_ctx[p->id] = NULL;

  return HANDLER_GO_ON;
}

/* this function is called at dlopen() time and inits the callbacks */

int mod_h264_streaming_plugin_init(plugin *p) {
  p->version     = LIGHTTPD_VERSION_ID;
  p->name        = buffer_init_string("h264_streaming");

  p->init        = mod_h264_streaming_init;
  p->handle_physical = mod_h264_streaming_path_handler;
  p->set_defaults  = mod_h264_streaming_set_defaults;
  p->cleanup     = mod_h264_streaming_free;

  p->handle_trigger = mod_h264_streaming_trigger;

  p->connection_reset = mod_h264_streaming_cleanup;
  p->handle_connection_close = mod_h264_streaming_cleanup;

  p->data        = NULL;

  return 0;
}

