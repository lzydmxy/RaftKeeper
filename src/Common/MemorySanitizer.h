/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <common/defines.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#endif

#define __msan_unpoison(X, Y)
#define __msan_test_shadow(X, Y) (false)
#define __msan_print_shadow(X, Y)
#define __msan_unpoison_string(X)

#if defined(ch_has_feature)
#    if ch_has_feature(memory_sanitizer)
#        undef __msan_unpoison
#        undef __msan_test_shadow
#        undef __msan_print_shadow
#        undef __msan_unpoison_string
#        include <sanitizer/msan_interface.h>
#    endif
#endif

#ifdef __clang__
#pragma clang diagnostic pop
#endif
