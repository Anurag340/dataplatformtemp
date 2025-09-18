#### Summary - 

Overview page we will need the following :  (Custom Tables)
1. Client Table - Refer excel. 
2. Modules Table - Refer excel. 
3. Stage table - Refer Execl. 
4. Goal table - Refer excel.
5. Module Nuggets - Refer excel.
6. Users - Refer Excel.
7. Nuggets Table - Refer excel. 

#### Transformed tables - 
1. Transformed campaign details - [campaign_id, name, created_at(timestamp), description, is_archived, is_enabled, is_draft, messages,  schedule_type, channels(list), first_sent(timestamp), last_sent(timestamp), goal, stage]
2. Transformed canvas details -   [canvas_id, name, created_at(timestamp), description, is_archived, is_enabled, is_draft, messages,  schedule_type, channels(list), first_sent(timestamp), last_sent(timestamp), goal, stage]
3. Transformed Variant table - [variant_id(primary key), id, type(campaign/canvas),time (all fields on left are custom), variant_name, sent, direct_opens, total_opens, bounces, body_clicks, revenue, unique_recipients, conversions, is_control_group(boolean)]

12/1/25
1. Transformed steps - [time, canvas_id, step_id, step_name, revenue, conversions, channels[list], unique_recipients, sent[extract & aggregate over channel objects], total_opens[extract & aggregate over channel objects],  
                   clicks[extract & aggregate over channel objects], unsubscribe[extract & aggregate over channel objects], delivered[extract & aggregate over channel objects] ]

   
#### Connector Tables. 
3. Mixpanel events - [event, time(int), distinct_id(user), insert_id(id given to every event), properties, metadata] (All strings)
