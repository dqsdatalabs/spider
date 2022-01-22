# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'immobilierquimpernestenn_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "immobilierquimpernestenn_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": "https://immobilier-quimper.nestenn.com/?agence_name_hidden=1602578811&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Appartement&type=Appartement&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "apartment"},
	        {"url": "https://immobilier-quimper.nestenn.com/?agence_name_hidden=1602578803&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Maison&type=Maison&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    def parse(self, response):
    
        for follow_url in response.xpath("//div[@id='annonce_entete_right']//script/text()").extract():
            main_url = follow_url.split('<a href="')[1].split('"')[0]
            yield Request(main_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        prop_type = response.meta.get('property_type')
        studio_type = "".join(response.xpath("//div[contains(@class,'ariane_pc')][2]/a/text()").extract())
        if studio_type:
            if "Studio" in studio_type:
                prop_type = "studio"
        item_loader.add_value("property_type", prop_type)
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h2[@id='titre']//text()").get()
        item_loader.add_value("title", title.strip())
        item_loader.add_value("external_link", response.url)
        meters = "".join(response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'Entre')]/text()").extract())
        if meters :
            try:
                meters = meters.split("Entre")[1].split("m")[0]
                item_loader.add_value("square_meters", str(math.ceil(float(meters.strip().split("m²")[0].strip()))))
            except:
                pass
        elif not meters:
            meters = "".join(response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'m²')]/text()").extract())
            if meters :
                item_loader.add_value("square_meters", str(math.ceil(float(meters.strip().split("m²")[0].strip()))))

        price = "".join(response.xpath("//div[@class='pos_rela']/div/text()[contains(.,'€')]").extract())
        if price :
            item_loader.add_value("rent_string", price.replace(" ",""))
            # item_loader.add_value("currency", "EUR")

        desc = "".join(response.xpath("//div[@class='container']/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='slider_bien']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        external_id = "".join(response.xpath("//div[@id='ref']/text()").extract())
        if external_id :
            item_loader.add_value("external_id", external_id.split(":")[1].split("/")[0].strip())


        address = "".join(response.xpath("//div[contains(@class,'ariane_pc')][2]/a/text()").extract())
        if address :
            address_2 = address.split(" ")[-4] + " " + address.split(" ")[-3]  
            item_loader.add_value("address",address_2)
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
            item_loader.add_value("city", address.split(" ")[-4])             

        room = "".join(response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'chambre')]/text()").extract())
        if room :
            item_loader.add_value("room_count", room.strip().split(" ")[0])
        
        floor = "".join(response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'etage')]/text()").extract())
        if floor :
            item_loader.add_value("floor", floor.strip().split(" ")[0].strip())

        deposit = "".join(response.xpath("//p[@class='textAlign_C']/text()[contains(.,'garantie : ')]").extract())
        if deposit :
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].replace(" ",""))

        item_loader.add_xpath("energy_label", "//div[@id='consoEner']/text()")

        item_loader.add_value("landlord_name", "Immobilier Nestenn")
        item_loader.add_value("landlord_email", "quimper@nestenn.com")
        item_loader.add_value("landlord_phone", "02 98 52 74 42")

        furnished=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'meuble')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)

        elevator=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)

        terrace=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)

        balcony=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        
        parking =response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'garage') or contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        
        bathroom_count=response.xpath("//div[@id='tableInfo']/ul/li[contains(.,'de bain') or contains(.,'Salle(s) d')]/div[2]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip()) 
          
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
        yield item_loader.load_item()