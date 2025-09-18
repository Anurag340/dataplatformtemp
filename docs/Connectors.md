Connector's README 
Aim - is to define & outlay how are storing data in the initial set of X tables. 

#### Campaign - 
1. Data series api - 
   #### GET /sends/data_series

    **Request Arguments:**
    - `campaign_id` (Required): String - The campaign API identifier.
    - `send_id` (Required): String - The send API identifier.
    - `length` (Required): Integer - Max number of days to include in the returned series.
    - `ending_at` (Optional): Datetime - Date on which the data series should end.

    **Response Example:**
    {"message":"success","data":[{"time":"2025-01-11T00:00:00Z","messages":{"ios_push":[{"variation_name":"Variation A","sent":100,"delivered":90,"undelivered":10,"delivery_failed":5,"direct_opens":30,"total_opens":50,"bounces":2,"body_clicks":25,"revenue":100,"unique_recipients":80,"conversions":20}]}}]}

2. Details api. 
   #### GET /campaigns/details

    **Request Arguments:**
    - `campaign_id` (Required): String - The campaign API identifier.

    **Response Example:**
    {"message":"success","campaign":{"id":"campaign_identifier","name":"Campaign Name","status":"active","created_at":"2025-01-01T00:00:00Z"}}

3. List api
   #### GET /campaigns/list

    **Request Arguments:**
    None

    **Response Example:**
    {"message":"success","campaigns":[{"id":"campaign_identifier_1","name":"Campaign Name 1","is_api_campaign":true},{"id":"campaign_identifier_2","name":"Campaign Name 2","is_api_campaign":false}]}

#### Canvas
1.  Series api - 
  #### GET /canvas/data_series

    **Request Arguments:**

    - `canvas_id` (Required): String - The canvas API identifier.
    - `length` (Required): String - Maximum number of days before ending_at to include in the returned series (between 1 and 100).
    - `ending_at` (Optional): Datetime - Date on which the data series should end.

    **Response Example:**
    {"message":"success","data":[{}]}

2. Details api. 
   #### GET /canvas/details

    **Request Arguments:**
    - `canvas_id` (Required): String - The canvas API identifier.

    **Response Example:**
    {"message":"success","canvas":{"id":"canvas_identifier"}}

3. List api
   #### GET /canvases/list

    **Request Arguments:**
    None

    **Response Example:**
    {"message":"success","canvases":[{"id":"canvas_identifier_1"},{"id":"canvas_identifier_2"}]}



