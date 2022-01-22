# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from re import template
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'immobiliaresegalerba_it_disabled'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 0.2,
        "PROXY_TR_ON" : True,
    }
    external_source = "Immobiliaresegalerba_PySpider_italy"
    start_urls = ['https://api.bludelego.it/api/realestate/v1/immobili']  # LEVEL 1

    formdata = {


        "page": "1",
        "id_provincia": "118402",
        "id_comune": "0",
        "id_tipologia": "0",
        "tipo_contratto": "2",
        "numero_locali": "0",
        "riferimento": "",
        "mq_min": "0",
        "mq_max": "0",
        "prezzo_min": "0",
        "prezzo_max": "0",
        "order_by": "date_desc",
        "residenziale": "1",
    }

    headers = {


        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9,tr;q=0.8,fr;q=0.7",
        "appkey": "SEGALERBA-?KokJfT3yNTNNJt8N7kjfdihfe8t73ykjfe978?347358-njfokjhsfd9p8wyt4985y3",
        "authkey": "eyJtZXNzYWdlIjoiSldUIFJ1bGVzISIsImlhdCI6MTQ1OTQ0ODExOSwiZXhwIjoxNDU5NDU0NTE5fQ",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://immobiliaresegalerba.it",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
    }

    def start_requests(self):


        
        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            formdata=self.formdata,
            headers=self.headers,
        )



    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False

        data_all = json.loads((response.body).decode("latin1"))
        data = data_all["data"]["immobili_list"]
   

        for item in data:
            base_url = f"https://immobiliaresegalerba.it/ads/{item['id']}/"
            f_url = f"https://api.bludelego.it/api/realestate/v1/immobili/{item['id']}"

            if get_p_type_string(item["titolo"]):
                yield Request(f_url, callback=self.populate_item, headers = self.headers, meta={"property_type": get_p_type_string(item["titolo"]),"base_url":base_url})
            seen = True

        if page==2 or seen:
            self.formdata["page"] = str(page)
            yield FormRequest(
                url=self.start_urls[0],
                callback=self.parse,
                formdata=self.formdata,
                headers=self.headers,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('base_url'))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        

        data_all = json.loads((response.body).decode("latin1"))
        data = data_all["data"][0]

        item_loader.add_value("external_id",str(data["id"]))
        item_loader.add_value("zipcode",data["codice"])
        item_loader.add_value("latitude",data["latitudine"])
        item_loader.add_value("longitude",data["longitudine"])
        item_loader.add_value("address",data["Indirizzo"])
        item_loader.add_value("city",data["provincia"])
        item_loader.add_value("rent",data["prezzo_richiesto"])
        item_loader.add_value("room_count", data["camere"])
        item_loader.add_value("bathroom_count",data["bagni"])
        item_loader.add_value("title",f"Appartamento in affitto {data['frazione']}-{data['provincia']}")
        item_loader.add_value("description",data["disposizione_interna"])
        
        features = data["features_list"]
        if "Ascensore" in str(features):
            item_loader.add_value("elevator",True)

        if "Terrazzo" in str(features):
            item_loader.add_value("terrace",True)

        if "Balconi" in str(features):
            item_loader.add_value("balcony",True)    
        
        photos = data["foto_list"]
        photo_list = []
        for photo in photos:
            photo_list.append(photo["nome"])
        item_loader.add_value("external_images_count",len(photo_list))
        item_loader.add_value("images",photo_list)

        
        square_meters = data["totale_mq"].split(".")[0]
        item_loader.add_value("square_meters",square_meters)

        energy_label = data["classe_energetica_converted"]
        if not (energy_label == "Non specificata" or energy_label == "Non disponibile"):
            item_loader.add_value("energy_label",energy_label)
        

        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","Immobiliare Segalerba")
        item_loader.add_value("landlord_email","+39 010 2770607")
        item_loader.add_value("landlord_phone","info@immobiliaresegalerba.it")



        yield item_loader.load_item()



def get_p_type_string(p_type_string):
    if p_type_string and ("appartamento" in p_type_string.lower() or "bilocale" in p_type_string.lower() or "trilocale" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "attico" in p_type_string.lower():
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "loft" in p_type_string.lower() or "quadrilocale" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None


