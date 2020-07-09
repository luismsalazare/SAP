# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # CRUCE SAP - RMCARE

# %%
import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# %%
#CREDENCIALES 
server = 'sqlnessieqa.database.windows.net' 
database = 'bddwdatalakeqa' 
username = 'lsalazar' 
password = 'ZBwFdDpxXF5K' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


# %%
# QUERY 
queryVIAUFKST = "SELECT ZRMC_4000_AVISOS.AUFNR AS OS4000, VIAUFKST.EQUNR AS EQUIPO_SAP, VIAUFKST.ERDAT AS FECHA_OS, VIAUFKST.AUFNR AS NUMERO_OS, VIAUFKST.SERIALNR AS NUMERO_SERIE, VIAUFKST.QMNUM AS NUMERO_AD ,  AFIH.ILOAN , IFLOS.STRNO AS UTSAP  FROM [SAP].[VIAUFKST]      LEFT JOIN SAP.ZRMC_4000_AVISOS       ON VIAUFKST.QMNUM=ZRMC_4000_AVISOS.QMNUM   LEFT JOIN SAP.AFIH ON VIAUFKST.AUFNR=AFIH.AUFNR    LEFT JOIN SAP.ILOA ON AFIH.ILOAN=ILOA.ILOAN  LEFT JOIN  SAP.IFLOS ON ILOA.TPLNR=IFLOS.TPLNR            WHERE AUART='ZS08'  AND VERSN='1'   ORDER BY VIAUFKST.ERDAT DESC"
df = pd.read_sql(queryVIAUFKST, cnxn)


# %%
queryCOEP = "SELECT *  FROM [SAP].[COEP]   WHERE OBJNR LIKE 'OR000018%' AND GJAHR>2018 AND VRGNG='COIN'   OR OBJNR LIKE 'OR000018%' AND GJAHR>2018 AND VRGNG='KOAO'   ORDER BY GJAHR DESC" 
df_coep = pd.read_sql(queryCOEP, cnxn)


# %%
# CAMBIO DE FORMATO DE CAMPOS
df = df.assign(
    EQUIPO_SAP = df["EQUIPO_SAP"].astype("int"),
    NUMERO_OS = df["NUMERO_OS"].astype("int"),    
    OS4000 = df["OS4000"].str[3:]
    )


# %%
# DESGLOSE DE UTSAP
df["UBIC_TEC"]=df["UTSAP"]
UTexpand=df['UTSAP'].str.split("-", n = 5, expand = True)
df['UTSAP']=UTexpand[0]
df['PREMODELO']=UTexpand[1]
df['EQUIPO']=UTexpand[2]
df['IDCOMPONENTE']=UTexpand[4]
df['IDPOSICION']=UTexpand[5]


# %%
# IMPORTAR DIMENSIONES DE NOMBRE DE FAENA Y NOMBRE DE COMPONENTE
df2 = pd.read_csv (r'C:\Users\u1309260\Desktop\Dimensiones\DIM_FAENA.csv',delimiter=';')
df3 = pd.read_csv (r'C:\Users\u1309260\Desktop\Dimensiones\DIM_COMPONENTE.csv',delimiter=',')


# %%
# MERGE DE FACT TABLE CON DIMENSIONES
df= pd.merge(df,df2, left_on='UTSAP', right_on='IDUTSAP',how='left')
df = pd.merge(df,df3, left_on='IDCOMPONENTE', right_on='IDCOMPONENTE', how='left')


# %%
# FILTRADO DE COLUMNAS REQUERIDAS DIMENSIONES
df = df.loc[:, ["OS4000",'EQUIPO_SAP',"NUMERO_AD","NUMERO_OS","FECHA_OS","NUMERO_SERIE",'IDCOMPONENTE','IDPOSICION','NOMBRE_COMPONENTE','IDRMCARE','IDSCAA','IDKOMTRAX','UBIC_TEC','NOMBRE','NOMBREZONA','LAT','LONG']]

# %% [markdown]
# ## COSTOS OS
# %% [markdown]
# ### OS DESDE 2019

# %%
df_coep = df_coep.iloc[:,:60]
df_coep["BELNR"] = df_coep["BELNR"].astype(str)
df_coep["WOGBTR"] = df_coep["WOGBTR"].astype(int)
df_coep_un = df_coep.drop_duplicates()

# %% [markdown]
# ### HISTORICO

# %%
df_coep_h = pd.read_csv (r'C:\Users\u1309260\Desktop\Tableau\COEP_HIST_BASE.csv',encoding='latin-1',sep=";")


# %%
df_coep_h["BELNR"] = df_coep_h["BELNR"].astype(str)


# %%
df_costos_os = pd.concat([df_coep_un,df_coep_h])
df_costos_os = df_costos_os.loc[:,["OBJNR","VRGNG","WOGBTR","GJAHR","BELNR"]]


# %%
df_costos_round = df_costos_os.assign(
    round = round(df_costos_os.WOGBTR + 51,-2)

)


# %%
df_costos_os = df_costos_round.drop_duplicates(subset=["OBJNR","VRGNG","BELNR","round"])


# %%
df_costos_os["NUMERO_OS"] = df_costos_os.OBJNR.astype(str).str.lstrip("OR0000").astype(int)


# %%
df_costos_os = df_costos_os.assign(    
    WOGBTR = df_costos_os.WOGBTR.fillna("0").astype(int)
    )


# %%
df_costos_un = df_costos_os.groupby(["NUMERO_OS","VRGNG"])["WOGBTR"].sum().abs().reset_index(name="COSTO_REPARACION").sort_values("VRGNG",ascending=False)


# %%
df_costos_un = df_costos_un.drop_duplicates(subset=["NUMERO_OS"],keep="first")

# %% [markdown]
# ### MERGE OS

# %%
dfjoin_coep = pd.merge(df,df_costos_un,left_on="NUMERO_OS",right_on="NUMERO_OS",how="left")


# %%
dfjoin_coep["COSTO_REPARACION"] = dfjoin_coep["COSTO_REPARACION"].astype(str)


# %%
dfjoin_coep.to_csv (r'C:\Users\u1309260\Desktop\Tableau\DW_OS_4000.csv', index = None, header=1,sep=";")


# %%
dfjoin_coep.info()

# %% [markdown]
# ### RMCARE

# %%
df4 = pd.read_excel (r'C:\Users\u1309260\Desktop\RMCare\FACT_TIDY_PSTATUS.xlsx',delimiter=',')


# %%
df4 = df4.query("os_no.notnull()")


# %%
df4_loc = df4.loc[:,["os_no","PCT_COMP","USO_COMP","USO_EQP","IDRMCARE","TIPO_EQP","MODELO","SERIE","COMP","POS","TBO","FECHA_TIEMPO"]]


# %%
dfjoin=pd.merge(
    dfjoin_coep,df4_loc,left_on="NUMERO_OS",right_on="os_no",how="left"
    )


# %%
dfjoin = dfjoin.assign(
    USO_COMP=dfjoin["USO_COMP"].fillna(0).astype(int),
    USO_EQP=dfjoin["USO_EQP"].fillna(0).astype(int),
    FECHA_OS=pd.to_datetime(dfjoin["FECHA_OS"]),
)


# %%
dfjoin = dfjoin.assign(
    OS_YEAR=dfjoin["FECHA_OS"].dt.year, OS_MES=dfjoin["FECHA_OS"].dt.month
)


# %%
dfjoin.to_csv(
    r"C:\Users\u1309260\Desktop\Tableau\DW_SAP_RMC.csv", index=None, header=1, sep=";"
)


# %%
df_year = (
    dfjoin.groupby(["OS_YEAR"])
    .count()[["NUMERO_OS", "os_no"]]
    .sort_values(by="OS_YEAR", ascending=False)
)


# %%
df_year.assign(match180_osno=(df_year["os_no"] / df_year["NUMERO_OS"]) * 100,)


# %%
(
    dfjoin.groupby(["OS_YEAR","OS_MES"])
    .count()[["NUMERO_OS", "os_no"]]
    .sort_values(by="OS_YEAR", ascending=False)
)


# %%
df_year.assign(match180_osno=(df_year["os_no"] / df_year["NUMERO_OS"]) * 100,)

# %% [markdown]
# ### MASTER TABLE UT SAP - SERIAL RMC

# %%
df_eqp = dfjoin


# %%
df_eqp["UBIC_TEC"] = df_eqp.UBIC_TEC.str[:19]


# %%
df_eqp = df_eqp.loc[:, ["UBIC_TEC", "IDRMCARE_y", "MODELO", "SERIE"]]


# %%
df_eqp = df_eqp.query("MODELO.notnull()")


# %%
df_eqp = df_eqp.drop_duplicates(subset="UBIC_TEC", keep="first").sort_values(
    by="UBIC_TEC", ascending=True
)


# %%
df_eqp.query("UBIC_TEC == 'KRTRT-01930E2XX-837'")


# %%
df_eqp.to_csv(
    r"C:\Users\u1309260\Desktop\Tableau\UT_EQP.csv", index=None, header=1, sep=";"
)

# %% [markdown]
# ### INTEGRACIONES

# %%
# IMPORTAR DATOS RMCARE
 pd.read_csv (r'C:\Users\u1309260\Desktop\Tableau\PlanStatus.csv',delimiter=',')
# FILTRADO DE COLUMNAS REQUERIDAS RMCARE
df4=df4.loc[:,["wo","cstatus","down_dt","c_life","eqp_life","site","eqptype","serial","comp","pst"]]
df4 = df4.query("wo.notnull()")
# MERGE SAP-RMCARE
dfjoin=pd.merge(df,df4,left_on="OS4000",right_on="wo",how="left")
dfjoin = dfjoin.assign(
    c_life = dfjoin["c_life"].fillna(0).astype(int),
    eqp_life = dfjoin["eqp_life"].fillna(0).astype(int),
    FECHA_OS = pd.to_datetime(dfjoin["FECHA_OS"])
)

# %% [markdown]
# ## VALIDACIONES

# %%
dfpruebas = dfjoin


# %%
dfwo = dfpruebas.query("IDCOMPONENTE.notnull()")


# %%
dfwo.loc[:,["UBIC_TEC","IDRMCARE","NOMBRE_COMPONENTE","site","eqptype","serial","comp","pst"]]


# %%
df_o = dfwo.loc[:,["UBIC_TEC","IDRMCARE","NOMBRE_COMPONENTE","site","eqptype","serial","comp","pst"]]
df_o = df_o.drop_duplicates(subset=['UBIC_TEC'], keep='first')
df_notnull = df_o.query("site.notnull()").sort_values(by="NOMBRE_COMPONENTE",ascending=True)
df_null = df_o.query("site.isnull()").sort_values(by="NOMBRE_COMPONENTE",ascending=True)


# %%
df_notnull.to_csv (r'C:\Users\u1309260\Desktop\Tableau\DW_OS_4000_Prueba_notnull.csv', index = None, header=1,sep=";")
df_null.to_csv (r'C:\Users\u1309260\Desktop\Tableau\DW_OS_4000_Prueba_null.csv', index = None, header=1,sep=";")


# %%
dfpruebas.groupby(["EQUIPO_SAP","NOMBRE_COMPONENTE"]).count()[["OS4000","wo"]].sort_values(by="OS4000",ascending=False).head(5)


# %%
dfpruebas["OS_YEAR"] = dfpruebas["FECHA_OS"].dt.year
dfpruebas["OS_MES"] = dfpruebas["FECHA_OS"].dt.month


# %%
dfañomes = dfjoin.groupby(["OS_YEAR","OS_MES"]).count()[["NUMERO_OS","OS4000","wo"]].sort_values(by="OS_YEAR",ascending=False)


# %%
dfañomes.assign(
    match180_4000 = (dfañomes["OS4000"]/dfañomes["NUMERO_OS"])*100,
    match4000_wo = (dfañomes["wo"]/dfañomes["OS4000"])*100    
).head(20)


# %%
dfaños = dfjoin.groupby(["OS_YEAR"]).count()[["NUMERO_OS","OS4000","wo"]].sort_values(by="OS_YEAR",ascending=False)


# %%
dfaños.assign(
    match180_4000 = (dfaños["OS4000"]/dfaños["NUMERO_OS"])*100,
    match4000_wo = (dfaños["wo"]/dfaños["OS4000"])*100    
).head(20)


# %%
dfbars = dfjoin.query("IDCOMPONENTE == 'P01'").sort_values(by="c_life",ascending=False)


# %%
# Basic plot
import numpy as np
import matplotlib.pyplot as plt
height = dfbars["c_life"]
bars = dfbars["IDCOMPONENTE"]
y_pos = np.arange(len(bars))
plt.bar(y_pos, height)
 
# If we have long labels, we cannot see it properly
names = dfbars["NOMBRE_COMPONENTE"]
plt.xticks(y_pos, names, rotation=90)
 
# Thus we have to give more margin:
plt.subplots_adjust(bottom=0.1)
 
# It's the same concept if you need more space for your titles
plt.title("PRUEBA DE GRAFICO")
plt.subplots_adjust(top=1)


# %%
# library & dataset
import seaborn as sns
df = sns.load_dataset('iris')
 
# use the function regplot to make a scatterplot
sns.regplot(x=dfbars["c_life"], y=dfbars["eqp_life"])
#sns.plt.show()
 
# Without regression fit:
sns.regplot(x=dfbars["c_life"], y=dfbars["eqp_life"], fit_reg=False)
#sns.plt.show()



# %%
# library & dataset
import seaborn as sns
df = sns.load_dataset('iris')
 
# use the function regplot to make a scatterplot
sns.regplot(x=dfbars["c_life"], y=dfbars["eqp_life"])
#sns.plt.show()
 
# Without regression fit:
sns.regplot(x=dfbars["c_life"], y=dfbars["eqp_life"], fit_reg=True)
#sns.plt.show()


# %%


