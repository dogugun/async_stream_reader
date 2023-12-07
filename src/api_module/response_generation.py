from variables.variables import RESPONSE_TIME_FORMAT


def generate_resp_object(df):
    df[["user.id", "user_name", "age"]]
    df_user = df[["user.id", "user_name", "age"]].drop_duplicates()
    df_event = df[['event', 'show.platform', 'show.title', 'first_actor', 'show.show_id', 'event_date']]

    user_obj = {"user_id": str(int(df_user["user.id"].values[0])),
                "user_name": df_user["user_name"].values[0],
                "age": str(int(df_user["age"].values[0]))}

    show_list_obj = [{"event": row["event"]
                      , "show_platform": row["show.platform"]
                      , "show_title": row["show.title"]
                      , "first_actor": row["first_actor"]
                      , "show_id": row["show.show_id"]
                      , "event_date": row["event_date"].strftime(RESPONSE_TIME_FORMAT).replace("000", "")} for i, row in df_event.iterrows()]

    resp_obj = {"user": user_obj, "event_list": show_list_obj}
    return resp_obj


def generate_resp_for_int(key, val):
    return {key: str(val)}


def generate_resp_successful_streams(df):
    resp_obj = [{"user": row["user.id"]
         , "successful_streams": row["count"]} for i, row in df.iterrows()]
    return resp_obj