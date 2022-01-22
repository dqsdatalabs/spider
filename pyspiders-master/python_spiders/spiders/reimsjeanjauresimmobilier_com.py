# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import math

class MySpider(Spider):
    name = 'reimsjeanjauresimmobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr' 
    external_source='Reimsjeanjauresimmobilier_PySpider_france_fr' 
    # custom_settings = {
    #     "PROXY_ON": True,
    #     "PASSWORD": "wmkpu9fkfzyo",
    # }

    def start_requests(self):
        start_urls = [
            {"url": "https://immobilier-reims-jean-jaures.nestenn.com/?agence_name_hidden=1603014235&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Appartement&type=Appartement&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "apartment"},
        ]  
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})


    def parse(self, response):
        url = response.xpath("//div[@id='annonce_entete_right']//a")
        for item in url:
            follow_url = response.urljoin(item.xpath(".//@href").extract_first())
            # prop =  item.xpath("./div[@class='infoAnnonce']/p[1]/text()").extract_first()
            # if prop:
            #     address =  prop.split(" ")[1]
            #     zipcode = prop.split(" ")[1].split(" ")[0]
            #     city = prop.split(zipcode)[1].strip()
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
        


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath("//h2[@id='titre']//text()").get()
        if title:
            item_loader.add_value("external_source", self.external_source)

            title = response.xpath("//h2[@id='titre']//text()").get()
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("address", "{} {}".format(response.meta.get("city"),response.meta.get("address")))
            item_loader.add_value("title", title)
            item_loader.add_value("zipcode",  response.meta.get("zipcode"))
            item_loader.add_value("city", response.meta.get("city"))
            item_loader.add_value("external_link", response.url)

            price=response.xpath("//div[@class='content_prix']//text()").get()
            if price:
                if " " in price:
                    price = price.replace(" ",".")
                item_loader.add_value("rent",price)
            item_loader.add_value("currency","EUR")

            square_meters=response.xpath("//div[@id='groupeIcon']/div[@class='blockIcon col-md-2 col-sm-3 col-xs-4']/div/text()[contains(.,'m²')]").extract_first()
            if square_meters:
                square_meters = square_meters.split("m²")[0].strip()
                square_meters = math.ceil(float(square_meters))
                item_loader.add_value("square_meters", str(square_meters))

            room_count=response.xpath("//div[@id='groupeIcon']/div[@class='blockIcon col-md-2 col-sm-3 col-xs-4']/div/text()[contains(.,'piece')]").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count.split("piece")[0]) 
            floor=response.xpath("//div[@id='groupeIcon']/div[@class='blockIcon col-md-2 col-sm-3 col-xs-4']/div/text()[contains(.,'etage')]").extract_first()
            if floor:
                item_loader.add_value("floor", floor.strip().split(" ")[0])     
            bathroom_count=response.xpath("//div[@id='tableInfo']/ul/li[contains(.,'de bain')]/div[2]/text()").extract_first()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip()) 

            external_id="".join(response.xpath("//div[@class='c_bleu']/text()[contains(.,'Réf')]").extract())
            if external_id:
                item_loader.add_value("external_id", external_id.split("Réf :")[1].strip())
            

            item_loader.add_xpath("energy_label", "//div[@id='consoEner']/text()[.!='NI' and .!='NC']")
            deposit=response.xpath("//p[@class='textAlign_C']/text()[contains(.,'Dépôt de garantie')]").extract_first()
            if deposit:
                item_loader.add_value("deposit", deposit.replace(" ",""))

            desc = "".join(response.xpath("//div[@class='container']/p/text()").extract())
            if desc:
                item_loader.add_value("description", desc)
                if "meublé " in desc.lower():
                    item_loader.add_value("furnished", True)

            images = [response.urljoin(x)for x in response.xpath("//div[@class='box_show box_show_images']//a/@href").extract()]
            if images:
                    item_loader.add_value("images", images)
            coordinat = response.xpath("//script/text()[contains(.,'coordinates')]").extract_first() 
            if coordinat:
                try:
                    map_coor = coordinat.split('coordinates": [')[1].split("]")[0]
                    lng = map_coor.split(",")[0].strip()
                    lat = map_coor.split(",")[1].strip()
                    item_loader.add_value("latitude", lat)
                    item_loader.add_value("longitude", lng)
                except:
                    pass
            
            terrace=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'terrasse')]/text()").get()
            if terrace:
                item_loader.add_value("terrace",True)

            balcony=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'balcon')]/text()").get()
            if balcony:
                item_loader.add_value("balcony",True)
            
            parking =response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'garage') or contains(.,'parking')]/text()").get()
            if parking:
                item_loader.add_value("parking",True)
                
            item_loader.add_value("landlord_phone", "03 10 45 01 30")
            item_loader.add_value("landlord_email", "reims-tinqueux@nestenn.com")
            item_loader.add_value("landlord_name", "Immobilier Marne")
        
        
            yield item_loader.load_item()

