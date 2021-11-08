
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

        if field == '100':
            name_SM = {'high': 'Alex5_22Apr16_28Jul16_VWC.csv', 'low': 'Philip1_22Apr16_28Jul16_VWC.csv'}
            name_SM_2 = {'high': 'Alex6_22Apr16_28Jul16_VWC.csv', 'low': 'Philip2_22Apr16_28Jul16_VWC.csv'}
            field_2 = 'W1'
        if field == '300':
            name_SM = {'high': 'EM32376_22Apr16_28Jul16_VWC.csv', 'low': 'EM32376_22Apr16_28Jul16_VWC.csv'}
            field_2 = 'W3'

        df = pd.io.parsers.read_csv(os.path.join(path, field_2 + '.csv'), header=[0, 1], sep=';')
        df = df.set_index(pd.to_datetime(df['None']['None']))
        df_reindexed = df.reindex(pd.date_range(start=df.index.min(), end=df.index.max(), freq='10Min')).interpolate(kind='cubic', axis=0)

        if field == '100':
            df_reindexed.rename(columns={'W1_1':'100_high'}, inplace=True)
            df_reindexed.rename(columns={'W1_2':'100_low'}, inplace=True)
            df_reindexed.rename(columns={'W1':'100'}, inplace=True)
        elif field == '300':
            df_reindexed.rename(columns={'W3_1':'300_high'}, inplace=True)
            df_reindexed.rename(columns={'W3_2':'300_low'}, inplace=True)
            df_reindexed.rename(columns={'W3':'300'}, inplace=True)


        for key in name_SM:
            data_SM = pd.read_csv(os.path.join(path_SM, name_SM[key]))
            data_SM['date'] = data_SM['date'] = pd.to_datetime(data_SM['date'])
            data_SM = data_SM.set_index('date')
            data_SM = data_SM.rename(columns={'date': 'date', 'Port1_VWC': 'Port1_SM', 'Port2_VWC': 'Port2_SM', 'Port3_VWC': 'Port3_SM', 'Port4_VWC': 'Port4_SM', 'Port5_VWC': 'Port5_SM'})
            del data_SM.index.name

            try:
                data_SM_2 = pd.read_csv(os.path.join(path_SM, name_SM_2[key]))
                data_SM_2['date'] = data_SM_2['date'] = pd.to_datetime(data_SM_2['date'])
                data_SM_2 = data_SM_2.set_index('date')
                data_SM_2 = data_SM_2.rename(columns={'date': 'date', 'Port1_VWC': 'Port1_SM_2', 'Port2_VWC': 'Port2_SM_2', 'Port3_VWC': 'Port3_SM_2', 'Port4_VWC': 'Port4_SM_2', 'Port5_VWC': 'Port5_SM_2'})
                data_SM_3 = pd.concat([data_SM, data_SM_2], axis=1)


                # df_SM[field + ' ' + key,'SM 5cm'] = data_SM_3[['Port1_SM', 'Port2_SM', 'Port3_SM', 'Port3_SM_2']].mean(axis=1)
            except:
                pass

            if name_SM[key] == 'Alex5_22Apr16_28Jul16_VWC.csv':
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM_3[['Port1_SM','Port2_SM','Port1_SM_2','Port2_SM_2','Port3_SM_2','Port4_SM_2','Port5_SM_2']].mean(axis=1)

            elif name_SM[key] == 'Philip1_22Apr16_28Jul16_VWC.csv':
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM_3[['Port1_SM','Port1_SM_2','Port2_SM_2','Port3_SM_2','Port4_SM_2','Port5_SM_2']].mean(axis=1)

            elif (name_SM[key] == 'EM32376_22Apr16_28Jul16_VWC.csv') and (key == 'high'):
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port1_SM','Port2_SM','Port3_SM','Port4_SM']].mean(axis=1)

            elif (name_SM[key] == 'EM32376_22Apr16_28Jul16_VWC.csv') and (key == 'low'):
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port5_SM']]


            df_add = df_reindexed.filter(like='Dry biomass total kg/m2')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Wet biomass total kg/m2')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)

            df_add = df_reindexed.filter(like='Height [cm]')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Water content total kg/m2')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Water content total [%]')
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
    df_insitu.to_csv(os.path.join(save_path, 'mni_in_situ_2016.csv'), encoding='utf-8', sep=',', float_format='%.4f')

    return df_insitu

path = '/media/nas_data/FINISHED_PROJECTS/2016_Wallerfing_Campaign/field_data/field_measurements/vegetation/csv'

# SM path
path_SM = '/media/nas_data/FINISHED_PROJECTS/2016_Wallerfing_Campaign/field_data/field_measurements/soil_moisture/data/data/2016_summer'

# field names
fields = ['100', '300']


# ESU names
esus = ['high', 'low']


# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

df_insitu = insitu(path, path_SM,fields,esus,save_path)
