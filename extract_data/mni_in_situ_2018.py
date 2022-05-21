
import pandas as pd
import os
import pdb

### Gathering of in-situ measurements (soil moisture, LAI, height, VWC) within Panda dataframe
#------------------------------------------------------------------------------

def insitu(path,path_SM,fields,esus,save_path):

    df_new = pd.DataFrame()
    df_SM = pd.DataFrame(columns=pd.MultiIndex(levels=[[],[]], codes=[[],[]]))
    df_SM2 = pd.DataFrame()

    for field in fields:
        df_SM = pd.DataFrame(columns=pd.MultiIndex(levels=[[],[]], codes=[[],[]]))

        if field == '317':
            name_SM = {'high': 'Philip1_20Apr18_13Jul18_SM.csv', 'low': 'Philip3_20Apr18_13Jul18_SM.csv', 'med': 'Philip5_20Apr18_13Jul18_SM.csv'}
        if field == '508':
            name_SM = {'high': 'Alex4_17May18_29Sep18_SM.csv', 'low': 'Alex5_17May18_29Sep18_SM.csv', 'med': 'Alex6_17May18_29Sep18_SM.csv'}
        if field == '410':
            name_SM = {'high': 'Alex1_16May18_14Aug18_SM.csv', 'low': 'Alex2_16May18_14Aug18_SM.csv', 'med': 'Alex3_16May18_14Aug18_SM.csv'}
        if field == '525':
            name_SM = {'high': 'Stud1_20Apr18_13Jul18_SM.csv', 'low': 'Stud2_20Apr18_13Jul18_SM.csv', 'med': 'Stud1_20Apr18_13Jul18_SM.csv'}

        df = pd.io.parsers.read_csv(os.path.join(path, field + '.csv'), header=[0, 1], sep=';')
        df = df.set_index(pd.to_datetime(df['None']['None']))
        df_reindexed = df.reindex(pd.date_range(start=df.index.min(), end=df.index.max(), freq='10Min')).interpolate(kind='cubic', axis=0)

        for key in name_SM:
            data_SM = pd.read_csv(os.path.join(path_SM, name_SM[key]))
            data_SM['date'] = data_SM['date'] = pd.to_datetime(data_SM['date'])
            data_SM = data_SM.set_index('date')
            del data_SM.index.name

            if name_SM[key] == 'Philip1_20Apr18_13Jul18_SM.csv':
                data_SM_2 = pd.read_csv(os.path.join(path_SM, 'Philip2_20Apr18_13Jul18_SM.csv'))
                data_SM_2['date'] = data_SM_2['date'] = pd.to_datetime(data_SM_2['date'])
                data_SM_2 = data_SM_2.set_index('date')
                data_SM_2 = data_SM_2.rename(columns={'date': 'date2', 'Port1_SM': 'Port1_SM_2', 'Port2_SM': 'Port2_SM_2', 'Port3_SM': 'Port3_SM_2', 'Port4_SM': 'Port4_SM_2', 'Port5_SM': 'Port5_SM_2'})
                data_SM_3 = pd.concat([data_SM, data_SM_2], axis=1)

                df_SM[field + ' ' + key,'SM 5cm'] = data_SM_3[['Port1_SM', 'Port2_SM', 'Port3_SM', 'Port3_SM_2']].mean(axis=1)

            elif name_SM[key] == 'Philip3_20Apr18_13Jul18_SM.csv':
                data_SM_2 = pd.read_csv(os.path.join(path_SM, 'Philip4_20Apr18_13Jul18_SM.csv'))
                data_SM_2['date'] = data_SM_2['date'] = pd.to_datetime(data_SM_2['date'])
                data_SM_2 = data_SM_2.set_index('date')
                data_SM_2 = data_SM_2.rename(columns={'date': 'date2', 'Port1_SM': 'Port1_SM_2', 'Port2_SM': 'Port2_SM_2', 'Port3_SM': 'Port3_SM_2', 'Port4_SM': 'Port4_SM_2', 'Port5_SM': 'Port5_SM_2'})
                data_SM_3 = pd.concat([data_SM, data_SM_2], axis=1)

                df_SM[field + ' ' + key,'SM 5cm'] = data_SM_3[['Port1_SM', 'Port2_SM', 'Port3_SM', 'Port2_SM_2', 'Port3_SM_2']].mean(axis=1)

            elif name_SM[key] == 'Philip5_20Apr18_13Jul18_SM.csv':
                data_SM_2 = pd.read_csv(os.path.join(path_SM, 'Philip6_20Apr18_13Jul18_SM.csv'))
                data_SM_2['date'] = data_SM_2['date'] = pd.to_datetime(data_SM_2['date'])
                data_SM_2 = data_SM_2.set_index('date')
                data_SM_2 = data_SM_2.rename(columns={'date': 'date2', 'Port1_SM': 'Port1_SM_2', 'Port2_SM': 'Port2_SM_2', 'Port3_SM': 'Port3_SM_2', 'Port4_SM': 'Port4_SM_2', 'Port5_SM': 'Port5_SM_2'})
                data_SM_3 = pd.concat([data_SM, data_SM_2], axis=1)

                df_SM[field + ' ' + key,'SM 5cm'] = data_SM_3[['Port1_SM', 'Port2_SM', 'Port3_SM', 'Port1_SM_2', 'Port2_SM_2', 'Port3_SM_2']].mean(axis=1)
            else:
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port1_SM', 'Port2_SM']].mean(axis=1)

            df_add = df_reindexed.filter(like='Dry biomass total kg/m2')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Wet biomass total kg/m2')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)

            df_add = df_reindexed.filter(like='LAI HANTS')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Height mean')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Water content total kg/m2')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Water content sample total [%]')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)

            df_SM2 = df_SM2.append(df_SM)



        # df_add = df_reindexed.filter(like='LAI mean HANTS')
        # df_new = df_new.append(df_add)
        # df_add = df_reindexed.filter(like='Height [cm] mean')
        # df_new = df_new.append(df_add)
        # df_add = df_reindexed.filter(like='Water content total kg/m2 mean HANTS')
        # df_new = df_new.append(df_add)
        # df_add = df_reindexed.filter(like='Water content total [%]')
        # df_new = df_new.append(df_add)
        # df_add = df_reindexed.filter(like='Dry biomass total kg/m2 mean HANTS')
        # df_add = df_add.filter(like=key)
        # df_new = df_new.append(df_add)
        # df_add = df_reindexed.filter(like='Wet biomass total kg/m2 mean HANTS')
        # df_add = df_add.filter(like=key)
        # df_new = df_new.append(df_add)


    df_new = df_new.append(df_SM2)
    df_insitu = df_new.groupby(df_new.index).mean()

    save_path = '/media/tweiss/Work/z_final_mni_data_2017'
    df_insitu.to_csv(os.path.join(save_path, 'mni_in_situ_2018.csv'), encoding='utf-8', sep=',', float_format='%.4f')

    return df_insitu


# LAI path
path = '/media/nas_data/2018_MNI_campaign/field_data/field_measurements/vegetation/revised/csv'

# SM path
path_SM = '/media/nas_data/2018_MNI_campaign/field_data/field_measurements/soil_moisture/data_revised'

# field names
fields = ['317', '410', '508', '525']
# fields = ['410']

# ESU names
esus = ['high', 'low', 'med']
# esus = ['high']

# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

df_insitu = insitu(path,path_SM,fields,esus,save_path)
