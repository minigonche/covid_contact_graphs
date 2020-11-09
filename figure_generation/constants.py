# Constants

reports_folder_location = "/home/jupyter/Dropbox/covid_fb/report"
figure_folder_name = "report_network_figures"
ident = '      '
date_format = "%Y-%m-%d"



import matplotlib.pyplot as plt
fig, ax = plt.subplots(1,1, figsize=(15.5, 7))
ax.plot(df_graph_attr_treatment.index.values,
        df_graph_attr_treatment.num_contacts_change_percentage.values, 
        color='k',
        label='%')

ax.plot(df_graph_attr_treatment.index.values,
        df_graph_attr_treatment.num_contacts_change_percentage_weighted.values, 
        color='r',
        label=r'$\%$ graph_size weighted')

ax.set_xlabel('Date')
ax.set_ylabel('Cambio porcentual en los contactos [%]')
#ax.set_yscale('log')

ax.legend()
plt.show()