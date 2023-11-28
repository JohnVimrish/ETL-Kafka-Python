import psycopg2
from graph_send import create_charts
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage




def send_email_with_image(image_data):
   
    # Create an HTML email with the chart image embedded

    from_address = 'fcm.bizmetric@gmail.com'
    to_address = 'fcm.bizmetric@gmail.com'
    password = 'dqvdnysmbjhvpvtl'

    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = 'IP Chart for 2023'

    count_of_charts = 0 
    for element in image_data:
         count_of_charts+= 1 

    charts_width = """ 
    <tr>
         <td> <img src="cid:ip_chart_month" alt="First Chart width="400" height="300"">  </td><td> <img src="cid:ip_chart_month" alt="First Chart width="400" height="300""> </td>
    </tr>
    """ *count_of_charts

    body_content  =       """
        <html>
        <body>
            <p>Here's the IP chart for 2023:</p>
            <p> IP Chart Date Wise </p>""" + charts_width+ """
            # Create the HTML email body
    email_body = MIMEText(
  

        </body>  
        </html>
        """
    

    # Create the HTML email body
    email_body = MIMEText(body_content,'html')

    msg.attach(email_body)


    # # Attach the chart image as an inline attachment
    for element in image_data:
        img = MIMEImage(element)
        img.add_header('Content-ID', '<ip_chart_month>')
        msg.attach(img)

    # # Send the email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_address, password)
    server.sendmail(from_address, to_address, msg.as_string())
    server.quit()



def fetch_data_and_generate_chart( db_conf):

    def create_db_connection(db_params) :
        # Connect to the PostgreSQL database
        return  psycopg2.connect(**db_params)
      
    def fire_query  (cursor,query) :
        cursor.execute(query)
        return cursor.fetchall()
    
    def close_connection_ncursor(connection,cursor) :
         cursor.close
         connection.close()

    def  chart_input_array_append ( input_var:list,input):
            input_var.append(input)

    def form_chart_input_data(chart_title:str,xayis_title:str,yaxis_title:str,xaxis_data:tuple,yaxis_data:tuple) :
         return (chart_title,xayis_title,yaxis_title,xaxis_data,yaxis_data)

    # PostgreSQL SQL query
    internal_project_count_dwquery = "SELECT create_date::DATE, COUNT(internal_project_name) FROM bms_teaminc_cip.bmt_cip_master GROUP BY create_date::DATE ORDER BY 1 desc"
    internal_project_count_mwquery = """
        SELECT concat(y, '-',   case WHEN m =1 THEN 'January'
                    WHEN m =2 THEN 'February'
                    WHEN m =3 THEN 'March'
                    WHEN m =4 THEN 'April'
                    WHEN m =5 THEN 'May'
                    WHEN m =6 THEN 'June'
                    WHEN m =7 THEN 'July'
                    WHEN m =8 THEN 'August'
                    WHEN m =9 THEN 'September'
                    WHEN m =10 THEN 'October'
                    WHEN m =11 THEN 'November'
                    WHEN m =12 THEN 'December'   END) mon,
            COUNT(internal_project_name) count_project
        FROM (
                SELECT internal_project_name,
                    EXTRACT( YEAR  FROM create_date   ) Y,
                    EXTRACT( MONTH FROM create_date ) m
                FROM bms_teaminc_cip.bmt_cip_master
            ) cip_m
        GROUP BY y,m
        ORDER BY y desc,m desc
       """

    postgres_connection  = create_db_connection(db_conf)
    db_cursor = postgres_connection.cursor()

    # Convert the query results into separate lists
    month_names, month_count = zip(*fire_query(db_cursor,internal_project_count_dwquery ))
    my_names, my_count = zip(*fire_query(db_cursor,internal_project_count_mwquery))

    close_connection_ncursor(postgres_connection,db_cursor)
    
    bar_graph_inputs  = list()
    plot_graph_inputs = list()
    
    chart_input_array_append(bar_graph_inputs,form_chart_input_data('title 1','xais1','yasis1',month_names, month_count))
    chart_input_array_append(plot_graph_inputs,form_chart_input_data('title 2','xais2','yasis2',my_names, my_count))

    chart_input  = {'bar':bar_graph_inputs ,'plot':plot_graph_inputs}
    send_email_with_image(create_charts(chart_input))

if __name__ == '__main__':
    
    # Database connection parameters
    db_confirguration = {
        "host": "192.168.168.12",
        "database": "bmd_fcm_cin_teaminc_sit",
        "user": "postgres",
        "password": "bizm4321"}
    
    fetch_data_and_generate_chart(db_confirguration)
