# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobilier_notaires'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Immobilier_Notaires_PySpider_france"
    
    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'Accept': 'application/json, text/plain, */*',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.immobilier.notaires.fr/fr/annonces-immobilieres-liste?page=1&parPage=12&typeTransaction=LOCATION&typeBien={}',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
        'Cookie': 'FOINTERMIN=A; has_js=1; tarteaucitron=u0021analytics=trueu0021googletagmanager=trueu0021youtube=true; _ga=GA1.3.71392353.1632812040; _gid=GA1.3.1706477708.1632812040; gaTrackEvent=false'
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobilier.notaires.fr/pub-services/inotr-www-annonces/v1/annonces?typeTransactions=LOCATION&typeBiens=APP&offset=0&page=1&parPage=12&perimetre=0",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immobilier.notaires.fr/pub-services/inotr-www-annonces/v1/annonces?typeTransactions=LOCATION&typeBiens=MAI&offset=0&page=1&parPage=12&perimetre=0"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                headers = {
                    'Connection': 'keep-alive',
                    'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
                    'Accept': 'application/json, text/plain, */*',
                    'sec-ch-ua-mobile': '?0',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
                    'sec-ch-ua-platform': '"Windows"',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Dest': 'empty',
                    'Referer': f"https://www.immobilier.notaires.fr/fr/annonces-immobilieres-liste?page=1&parPage=12&typeTransaction=LOCATION&typeBien={(item.split('typeBiens=')[1].split('&')[0])}",
                    'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
                    'Cookie': 'FOINTERMIN=A; has_js=1; tarteaucitron=u0021analytics=trueu0021googletagmanager=trueu0021youtube=true; _ga=GA1.3.71392353.1632812040; _gid=GA1.3.1706477708.1632812040; gaTrackEvent=false'
                }
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=headers,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)["annonceResumeDto"]
        page = response.meta.get('page', 2)
        
        seen = False
        for item in data:
            if "urlDetailAnnonceFr" in item:
                yield Request(
                    item['urlDetailAnnonceFr'], 
                    callback=self.populate_item, 
                    meta={
                        "property_type": response.meta.get('property_type'),
                        "data": item
                    }
                )
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"&page={page-1}", f"&page={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        data = response.meta.get('data') #bazi bilgiler jsondan alinabilir.
 
        id=data['reference']
        if id:
            item_loader.add_value("external_id",str(id))
        city=data['localiteNom']
        if city:
            item_loader.add_value("city",city)
            item_loader.add_value("address",city)
        zipcode=data['codePostal']
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        rent=data["prixAffiche"]
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        description=data["descriptionFr"]
        if description:
            item_loader.add_value("description",description)
        if "surface" in data:
            surface=data['surface']
            if surface:
                item_loader.add_value("square_meters",surface)
        room_count=data['nbPieces']
        if room_count:
            item_loader.add_value("room_count",room_count)
        phone=data['telephone']
        if phone:
            item_loader.add_value("landlord_phone",phone)
       

        yield item_loader.load_item()