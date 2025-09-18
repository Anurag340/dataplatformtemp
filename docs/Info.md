Braze campaigns are split into 3 tables.

1. Campaign_data_series
2. Campaign_details
3. Campaign_list

Braze canvas are split into 3 tables.
1. Canvas_data_series (Contains information split by timed day objects wherein every day object contains total_stats, variant stats & step stats(this has all the required important information for us.)
2. Canvas_details
3. Canvas_list

Set of X tables above. 

Campaign Schema definition (input)
1. Data_series : [campaignId, data, metadata, created_at] [All strings] (Will feed into Variants)
2. Campaign_details : [campaignId, data, metadata, created_at] [All strings] (Will feed into transformed campaign details)
3. Campaign_list : [campaignId, name, tags, last_edited, is_api_campaign(Boolean), metadata, created_at]

Canvas Schema definition (input)
1. Canvas data series - [canvas_id, name, stats, metadata, created_at] (Will feed into Variants)
2. Canvas details - [canvas_id, data, metadata, created_at] (Will feed into transformed campaign details)
3. Canvas list - [id, name, tags, last_edited, metadata, created_at]


Transformation of details is done with the following fields - 
1. Transform_data_series (deprecate). [id(uuid), type_id(rename - campaign_id), type, name, description, type_created_at, type_updated_at, schedule, messages, channels, goal_id, stage_id, client_id, is_enabled, is_archived, tags, first_sent, last_sent, is_draft, variant_id(what's this ?) ]
2. new_transform_data_series: [id(uuid), type_id(rename - campaign_id), type, name, description, type_created_at(timestamp), type_updated_at(timestamp), schedule, messages, channels, is_enabled, is_archived, tags, first_sent, last_sent, is_draft, variant_id, control_group_id]
3. Transformed campaign details - [campaignId, name, created_at(timestamp), description, is_archived, is_enabled, is_draft, schedule_type, channels(list), first_sent, last_sent,  created_at(timestamp)]
4. Transformed canvas details - [] -> do we need this table ? 
5. Transformed Variant table - [variant_id(primary key), campaign_id, channel(email, ios_push, etc...), time (all fields on left are custom), sent, direct_opens, total_opens, bounces, body_clicks, revenue, unique_recipients, conversions, is_control_group(boolean)]

***