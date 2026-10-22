from pathlib import Path
import json, copy, hashlib
r=Path('.')
u={'kind':'urllib3','total':2,'status_forcelist':[503],'backoff_factor':0,'allowed_methods':None}
t={'kind':'tenacity','stop_attempts':2,'wait_kind':'fixed','wait_s':0.015,'on_exception':True}
policies={'single':{'layers':[u]},'composed':{'layers':[t,u]},'shared':{'layers':[{'kind':'budget','max_attempts':4,'deadline_s':1.5},t,u]}}
def status(n,ra=False):
 plan=[{'respond':503,**({'headers':{'Retry-After':'1'}} if ra else {})} for _ in range(n)]
 return {'scenario':{'plan':plan,'default':{'respond':200,'effect':'apply'}},'outcomes':[{'kind':'status','status':503,**({'retry_after_s':1} if ra else {})} for _ in range(n)]+[{'kind':'success'}]}
fails={'recover_2':status(2),'recover_4':status(4),'retry_after':status(1,True),'drop_status':{'scenario':{'plan':[{'drop':True,'effect':'apply'},{'respond':503}],'default':{'respond':200,'effect':'apply'}},'outcomes':[{'kind':'read_err'},{'kind':'status','status':503},{'kind':'success'}]},'delayed_effect':{'scenario':{'default':{'delay_s':0.18,'effect':'apply','then':{'respond':200}}},'outcomes':[{'kind':'read_err','delay_s':0.18}]},'connect':{'scenario_type':'dead_port','outcomes':[{'kind':'conn_err'}]}}
cells=[]
for f in fails:
 for p in policies:
  cells.append({'id':f'main_{f}_{p}','split':'eval','policy':p,'failure':f,'method':'POST','contract':{'max_transport_attempts':4,'max_effects':1,'max_wall_s':4,'deadline_s':1.5} if p=='shared' else {'max_transport_attempts':4,'max_effects':1,'max_wall_s':4}})
m={'matrix_version':'main-1.0','defaults':{'connect_timeout_s':0.15,'read_timeout_s':0.08,'reps':2,'contract':{'max_transport_attempts':4,'max_effects':1,'max_wall_s':4}},'policies':policies,'failures':fails,'cells':cells}
(r/'config/main_matrix.json').write_text(json.dumps(m,indent=2),encoding='utf-8')
# Separate instrumentation development check, excluded from confirmation.
s=copy.deepcopy(m);s['failures']={'development_recover_3':status(3)};s['cells']=[{'id':'development_recover_3','split':'calibration','policy':'composed','failure':'development_recover_3','method':'POST'}];s['defaults']['reps']=1
(r/'config/main_smoke.json').write_text(json.dumps(s,indent=2),encoding='utf-8')
print('confirmation cases',len(cells),'reps',len(cells)*2,'worst-case attempts',2*6*(3+6+4),'smoke max',6)
