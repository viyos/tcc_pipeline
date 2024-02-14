import instaloader
import google.generativeai as genai
import time
from random import seed
from random import randint
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow
from datetime import timedelta, datetime
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.blocks.system import Secret

@flow(log_prints=True)
def get_post_comments(username: str):
    login_insta = Secret.load("login-insta")
    password_insta = Secret.load("password-insta")
    api_key_googleia = Secret.load("api-key-googleia")
    login_mysql = Secret.load("login-mysql")
    password_mysql = Secret.load("password-mysql")
    count = 1
    #Credenciais de login Instagram
    L = instaloader.Instaloader()
    L.login(login_insta.get(), password_insta.get()) 
    posts = instaloader.Profile.from_username(L.context, username).get_posts()
    print("Conexão com o instagram realizada com sucesso.")
    #Credenciais de autenticação AI Google
    genai.configure(api_key=api_key_googleia.get())
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            pass
    model = genai.GenerativeModel('gemini-pro')
    print("Conexão com o Google IA realizada com sucesso.")
    #Credenciais MySQL
    db_data = 'mysql+mysqldb://' + str(login_mysql.get()) + ':' + str(password_mysql.get())  + '@' + '127.0.0.1/' + 'redes_sociais' + '?charset=utf8mb4'
    engine = create_engine(db_data)
    for post in posts:
        try:
            for comment in post.get_comments():
                response = model.generate_content(f"Me diga somente se o comentário a seguir é positivo, negativo ou neutro: {str(comment.text)}.")
                analise_sentimento = response.text.replace('.','')
                dictcomment = {"ID_Comment":int(comment.id),
                        "Comment":str(comment.text)}
                dfcomment = pd.DataFrame(dictcomment,index=[0])
                try:
                    dfcomment.to_sql('comments_insta' , engine, if_exists='append', index=False,method='multi')
                except :
                    print('Item duplicado.')
                dict = {"ID_Comment":int(comment.id),
                        "Datetime_Comment":str(comment.created_at_utc),
                        "Username":str(comment.owner.username),
                        "Full_Name" : str(comment.owner.full_name),
                        "Followers_User" :int(comment.owner.followers),
                        "Sentiment":str(analise_sentimento),
                        "Likes_Comment":int(comment.likes_count)}
                df = pd.DataFrame(dict,index=[0])
                try:
                    df.to_sql('sentiment_insta' , engine, if_exists='append', index=False,method='multi')
                except:
                    print('Item duplicado.')
                seed(1)
                tempo = randint(5,13) #Tempo de espera para realizar uma requisição sem ficar com o mesmo valor sequencial
                time.sleep(tempo)
                print(df)
        except:
            count = count + 1
            print('Pausa de conexão') #Tratar o erro HTTP error code 401.
            if count == 10:
                break
            else:
                time.sleep(30)

# Task
get_post_comments(username='hdisegurosbr')
             
if __name__ == "__main__":
    get_post_comments.serve(name="insta-comments",schedule=IntervalSchedule(interval=timedelta(days=1), 
                anchor_date=datetime(2024, 1, 1, 23, 00), 
                timezone="America/Sao_Paulo"))