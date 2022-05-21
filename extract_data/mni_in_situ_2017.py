
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

        if field == '301':
            name_SM = {'high': 'Alex4_23May17_17Jul17_SM.csv', 'low': 'Alex5_23May17_17Jul17_SM.csv', 'med': 'Alex6_23May17_17Jul17_SM.csv'}
        if field == '508':
            name_SM = {'high': 'Stud1_23May17_20Jul17_SM.csv', 'low': 'Stud2_23May17_20Jul17_SM.csv', 'med': 'blue_box_2017.csv'}
        if field == '542':
            name_SM = {'high': 'Alex1_23May17_20Jul17_SM.csv', 'low': 'Alex2_23May17_20Jul17_SM.csv', 'med': 'Alex3_23May17_19Jul17_SM.csv'}
        if field == '319':
            name_SM = {'high': 'Philip4_07Jun17_04Sep17_SM.csv', 'low': 'Philip5_07Jun17_04Sep17_SM.csv', 'med': 'Philip6_07Jun17_04Sep17_SM.csv'}
        if field == '515':
            name_SM = {'high': 'Philip1_07Jun17_22Sep17_SM.csv', 'low': 'Philip2_07Jun17_22Sep17_SM.csv', 'med': 'Philip3_07Jun17_22Sep17_SM.csv'}
        # pdb.set_trace()
        df = pd.io.parsers.read_csv(os.path.join(path, field + '.csv'), header=[0, 1], sep=';')
        df = df.set_index(pd.to_datetime(df['None']['None']))
        df_reindexed = df.reindex(pd.date_range(start=df.index.min(), end=df.index.max(), freq='10Min')).interpolate(kind='cubic', axis=0)

        for key in name_SM:
            data_SM = pd.read_csv(os.path.join(path_SM, name_SM[key]))
            data_SM['date'] = data_SM['date'] = pd.to_datetime(data_SM['date'])
            data_SM = data_SM.set_index('date')
            del data_SM.index.name

            if name_SM[key] == 'blue_box_2017.csv':
                data_SM.index = data_SM.index + pd.Timedelta('5 min')
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['5TM soil moisture addr 1', '5TM soil moisture addr 2', '5TM soil moisture addr 3']].mean(axis=1)/100.

            elif name_SM[key] == 'Philip6_07Jun17_04Sep17_SM.csv':
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port1_SM']].mean(axis=1)[df_SM.index]

            elif name_SM[key] == 'Philip1_07Jun17_22Sep17_SM.csv':
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port2_SM']].mean(axis=1)[df_SM.index]

            elif name_SM[key] == 'Philip2_07Jun17_22Sep17_SM.csv':
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port4_SM']].mean(axis=1)[df_SM.index]

            elif name_SM[key] == 'Philip3_07Jun17_22Sep17_SM.csv':
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port1_SM', 'Port2_SM', 'Port3_SM']].mean(axis=1)
            else:
                df_SM[field + ' ' + key,'SM 5cm'] = data_SM[['Port1_SM', 'Port2_SM']].mean(axis=1)

            df_add = df_reindexed.filter(like='Dry biomass total kg/m2 HANTS')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Wet biomass total kg/m2 HANTS')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)

            df_add = df_reindexed.filter(like='LAI HANTS')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Height [cm]')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Water content total kg/m2 HANTS')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)
            df_add = df_reindexed.filter(like='Water content total [%]')
            df_add = df_add.filter(like=key)
            df_new = df_new.append(df_add)

            df_SM2 = df_SM2.append(df_SM)

        df_add = df_reindexed.filter(like='LAI mean HANTS')
        df_new = df_new.append(df_add)
        df_add = df_reindexed.filter(like='Height [cm] mean')
        df_new = df_new.append(df_add)
        df_add = df_reindexed.filter(like='Water content total kg/m2 mean HANTS')
        df_new = df_new.append(df_add)
        df_add = df_reindexed.filter(like='Water content total [%]')
        df_new = df_new.append(df_add)
        df_add = df_reindexed.filter(like='Dry biomass total kg/m2 mean HANTS')
        df_add = df_add.filter(like=key)
        df_new = df_new.append(df_add)
        df_add = df_reindexed.filter(like='Wet biomass total kg/m2 mean HANTS')
        df_add = df_add.filter(like=key)
        df_new = df_new.append(df_add)


    df_new = df_new.append(df_SM2)
    df_insitu = df_new.groupby(df_new.index).mean()

    save_path = '/media/tweiss/Work/z_final_mni_data_2017'
    df_insitu.to_csv(os.path.join(save_path, 'mni_in_situ_2017.csv'), encoding='utf-8', sep=',', float_format='%.4f')

    return df_insitu


# LAI path
path = '/media/nas_data/2017_MNI_campaign/field_data/field_measurements/vegetation/revised/csv'

# SM path
path_SM = '/media/nas_data/2017_MNI_campaign/field_data/field_measurements/soil_moisture/data_revised'

# field names
fields = ['301', '508', '542', '319', '515']
# fields = ['508']

# ESU names
esus = ['high', 'low', 'med']
# esus = ['high']

# Save output path
save_path = '/media/tweiss/Work/z_final_mni_data_2017'

df_insitu = insitu(path,path_SM,fields,esus,save_path)
