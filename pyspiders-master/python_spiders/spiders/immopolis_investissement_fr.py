# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'immopolis_investissement_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "type" : 3,
                "property_type" : "house"
            },
            {
                "type" : 2,
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))

            payload = {
                "page": "0",
                "id_categorie_biens": "0",
                "id_types_biens": "0",
                "a_la_une": "false",
                "vendu": "0",
                "item_per_page": "50",
                "order_biens_page": "1",
                "TypeBienOption": r_type,
                "nb_pieces1": "0",
                "Budget_min": "0",
                "Budget_max": "0",
                "departement": "0",
                "type": "2",
            }
            
            yield FormRequest(url="https://www.immopolis-investissement.fr/immopolis/biens_pages.php",
                                callback=self.parse,
                                formdata=payload,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='article']/@href").extract():
            f_url = "https://www.immopolis-investissement.fr/" + item
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Immopolisinvestissement_PySpider_"+ self.country + "_" + self.locale)


        item_loader.add_value("property_type", response.meta.get('property_type'))  
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        rent="".join(response.xpath("//div[contains(@class,'row-caract')][contains(.,'Loyer')]/div[2]//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//div[contains(@class,'row-caract')][contains(.,'habitation')]/div[2]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count=response.xpath("//div[contains(@class,'row-caract')][contains(.,'chambres')]/div[2]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//div[contains(@class,'row-caract')][contains(.,'salles de bain')]/div[2]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        city = response.xpath("//div[contains(@class,'row-caract')][contains(.,'Ville')]/div[2]//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        address=response.xpath("//div[contains(@class,'row-caract')][contains(.,'Ville')]/div[2]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        zipcode=response.xpath("//div[contains(@class,'row-caract')][contains(.,'Code')]/div[2]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        external_id=response.xpath("substring-after(//div[@class='ref_detail_bien']/text(),':  ')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        desc="".join(response.xpath("//div[@class='texte_detail_bien']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        image="".join(response.xpath("//ul[@class='slides']/li/@style").getall())
        images=image.split('background-image:url(')
        image_size=len(images)
        for i in  range(1,image_size):
            item_loader.add_value("images", image.split('image:url(')[i].split("')")[0])
        item_loader.add_value("external_images_count", str(image_size))
        
        item_loader.add_value("landlord_name","Immopolis Investissement")
        item_loader.add_value("landlord_phone","05 61 62 76 88")
        item_loader.add_value("landlord_email","contact-toulouse@immopolis-investissement.com")
        
        energy_label=response.xpath("//div[@id='dpe_text']/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("_")[1])
            
        parking=response.xpath("//div[contains(@class,'row-caract')][contains(.,'parking')]/div[2]//text()").get()
        if parking:
            item_loader.add_value("parking",True)
        
        yield item_loader.load_item()

