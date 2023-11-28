import matplotlib.pyplot as plt
import io

def create_charts(input_data):

    def  create_chart (chart_type:str,input_array:tuple) :
        chart_title = input_array[0]
        xaxis_title = input_array[1]
        yaxis_title = input_array[2]
        xaxis_data =  input_array[3]
        yaxis_data =  input_array[4]
        
        # print (chart_type,chart_title,xaxis_title,yaxis_title,xaxis_data,yaxis_data )

        # Create a bar chart
        plt.figure(figsize=(5, 3))
        if chart_type == 'bar':
            plt.bar(xaxis_data, yaxis_data)
        if chart_type == 'plot':
            plt.plot(xaxis_data, yaxis_data)
        plt.xlabel(xaxis_title)
        plt.ylabel(yaxis_title)
        plt.title(chart_title)
        plt.xticks(rotation=45, ha='right')
        plt.subplots_adjust(bottom=0.3)
        plt.tight_layout()
        # plt.show()

        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        return buffer.read()

    
    charts  = list()    
    for  chart_type, chart_inputs in  input_data.items() :
       for every_chart_input in  chart_inputs :
            charts.append(create_chart(chart_type, every_chart_input))

    return charts
