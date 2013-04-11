%%%
%%% Copyright 2012, Erlang Solutions, Inc.  All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%


%%%-------------------------------------------------------------------
%%% File:      folsom_metrics_daily.erl
%%% @author    Michal Niec <michal.niec@erlang-solutions.com>
%%% @doc A total count, and sliding window count of events over the last
%%%      10 sec, 1 minute, 10 minutes, 1 hour, 1 day.
%%% @end
%%%------------------------------------------------------------------

-module(folsom_metrics_daily).

-export([new/1,
         update/2,
         trim/2,
         get_value/1,
         get_values/1
        ]).

%% size of the window in seconds
-define(WINDOW, 60).
%% size of second window in minutes
-define(MIN_WINDOW, 60*24).
-define(WIDTH, 16). %% Keep this a power of two

-include("folsom.hrl").

new(Name) ->
    Daily = #daily{},
    Pid = folsom_sample_slide_sup:start_slide_server(?MODULE,
                                                           Daily#daily.tid,
                                                           ?WINDOW),
    ets:insert_new(Daily#daily.tid,
                   [{{total, N}, 0} || N <- lists:seq(0,?WIDTH-1)]),
    ets:insert(?DAILY_TABLE, {Name, Daily#daily{server=Pid}}).

update(Name, Value) ->
    #daily{tid=Tid} = get_value(Name),

    Moment = folsom_utils:now_epoch(),
    Minute = Moment div 60,
    Rnd = get_rnd(),

    folsom_utils:update_counter(Tid, {sec, Moment, Rnd}, Value),
    folsom_utils:update_counter(Tid, {min, Minute, Rnd}, Value),
    ets:update_counter(Tid, {total, Rnd}, Value).

get_value(Name) ->
    [{Name, Daily}] =  ets:lookup(?DAILY_TABLE, Name),
    Daily.

trim(Tid, _Window) ->
    Oldest = folsom_utils:now_epoch() - ?WINDOW,
    OldestMin = Oldest div 60 - ?MIN_WINDOW, 
    
    ets:select_delete(Tid, [{{{sec,'$1','_'},'_'}, [{'<', '$1', Oldest}], ['true']}]),
    ets:select_delete(Tid, [{{{min,'$1','_'},'_'}, [{'<', '$1', OldestMin}], ['true']}]).

get_values(Name)->
    Now = folsom_utils:now_epoch(),
    TenSecAgo = Now - 10,
    MinAgo = Now - 60,
    TenMinAgo = Now div 60 - 10,
    HourAgoMin = Now div 60 - 60,

    #daily{tid=Tid} = get_value(Name),
    
    TenSec = lists:sum(ets:select(Tid, [{{{sec,'$1','_'},'$2'},
					 [{'>=', '$1', TenSecAgo}],['$2']}])),
    TenSecAvg = TenSec / 10,    
    Min    = lists:sum(ets:select(Tid, [{{{sec,'$1','_'},'$2'},
					[{'>=', '$1', MinAgo}],['$2']}])),
    TenMin = lists:sum(ets:select(Tid, [{{{min,'$1','_'},'$2'},
					 [{'>=', '$1', TenMinAgo}],['$2']}])),
    Hour   = lists:sum(ets:select(Tid, [{{{min,'$1','_'},'$2'},
					[{'>=', '$1', HourAgoMin}],['$2']}])),
    Day    = lists:sum(ets:select(Tid, [{{{min,'_','_'},'$1'},[],['$1']}])),    
    Total  = lists:sum(ets:select(Tid, [{{{total,'_'},'$1'},[],['$1']}])),
		      
    [{tenSecAVG, TenSecAvg},
     {tenSec, TenSec}, 
     {minSum, Min},
     {tenMinSum, TenMin},
     {hourSum, Hour},
     {daySum, Day},
     {total, Total}].


get_rnd()->
    X = erlang:system_info(scheduler_id),
    X band (?WIDTH-1).
