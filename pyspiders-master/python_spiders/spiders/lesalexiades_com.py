# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
import math 

class MySpider(Spider):  
    name = 'lesalexiades_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Lesalexiades_com_PySpider_france_fr"
    custom_settings = { 
        "HTTPCACHE_ENABLED": False,
    } 

    headers = {
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'cache-control': "no-cache",
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'host': "www.lesalexiades.com",
        'origin': "https://www.lesalexiades.com",
        'referer': "https://www.lesalexiades.com/recherche/",
        'sec-fetch-dest': "document",
        'sec-fetch-mode': "navigate",
        'sec-fetch-site': "same-origin",
        'sec-fetch-user': "?1",
        'upgrade-insecure-requests': "1",
    }

    def start_requests(self): 
        start_urls = [
            {
                "type" : 2,
                "property_type" : "house"
            },
            {
                "type" : 1,
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            data_dict = {
                "data[Search][offredem]": "2",
                "data[Search][idtype]": str(url.get("type")),
                "data[Search][idvillecode]": "void",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][prixmin]": "",
                "data[Search][prixmax]": "",
                "data[Search][surfmin]": "",
                "data[Search][surfmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][piecesmax]": "",
            }

            yield FormRequest(
                "https://www.lesalexiades.com/recherche/",
                formdata=data_dict,
                callback=self.parse,
                headers=self.headers,
                meta={
                    "property_type" : url.get("property_type"),
                },
            )
            

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='listingUL']/li"): 
            follow_url = response.urljoin(item.xpath(".//div[@class='bienTitle']/h1/a/@href").get())
            address = item.xpath(".//div[@class='bienTitle']/h2/text()").get()
            address = address.split("-")[-1].strip()
            property_type = response.meta.get("property_type")
            yield Request(follow_url, callback=self.populate_item, meta={'property_type' :property_type, "address" : address})
    
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        # item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = "".join(response.xpath("//div[@class='themTitle']/h1[@itemprop='name']//text()").extract())
        item_loader.add_value("title", title.strip())
        property_type = item_loader.get_output_value("title")
        if property_type:
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            elif "studio" in property_type.lower():
                item_loader.add_value("property_type","studio")
            elif "maison" in property_type.lower():
                item_loader.add_value("property_type","house")
            else:
                return

        status = response.xpath("//h1[contains(.,'Cave') or contains(.,'Parking')]/text()").get()
        r_status = response.xpath("//h1/span[@class='prix']/span/text()").get()
        if r_status and "CC*" in r_status and (not status):       
            item_loader.add_value("external_source", self.external_source)

            price = "".join(response.xpath("//span[@class='prix']/text()").re(r"\d+"))
            if price:
                item_loader.add_value("rent", price)
                item_loader.add_value('currency', "EUR")

            city = "".join(response.xpath("//span[contains(.,'Ville')]/following-sibling::span//text()").getall())
            if city:
                item_loader.add_value("city", city.replace("\n", "").replace("\xa0", "").replace("'", "").strip())

            
            external_id = response.xpath("//span[@class='ref']/text()").extract_first()
            if external_id:
                item_loader.add_value("external_id", external_id.replace("Ref","").strip())

            zipcode = response.xpath("//p[@class='data']/span[contains(.,'Code postal')]/following-sibling::span/text()").extract_first()
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())

            floor = response.xpath("//p[@class='data']/span[contains(.,'Etage')]/following-sibling::span/text()").extract_first()
            if floor:
                item_loader.add_value("floor", floor.strip())
    
            item_loader.add_value("address", response.meta.get("address"))

            room_count = response.xpath("//p[@class='data']/span[contains(.,'chambre')]/following-sibling::span/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            roomcheck=item_loader.get_output_value("room_count")
            if not roomcheck:
                room=response.xpath("//div[@class='bienTitle themTitle']/h1/text()").get()
                if room:
                    item_loader.add_value("room_count",room.split("pi")[0].split("-")[1].replace("\n",""))
            bathroom_count = response.xpath("//p[@class='data']/span[contains(.,'Nb de salle d')]/following-sibling::span/text()").extract_first()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
            

            square = response.xpath("//p[@class='data']/span[contains(.,'Surface ')]/following-sibling::span/text()").extract_first()
            if square:
                square =square.split("m")[0].replace(",",".").strip()
                square_meters = math.ceil(float(square.strip()))
                item_loader.add_value("square_meters",square_meters )
                
            desc = "".join(response.xpath("//p[@itemprop='description']//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())
    
    
            deposit = response.xpath("//p[@class='data']/span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").extract_first()
            if deposit:
                item_loader.add_value("deposit", deposit.split("€")[0].strip())
            utilities = response.xpath("//p[@class='data']/span[contains(.,'Charges')]/following-sibling::span/text()").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€")[0].strip()) 
            images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'carousel-inner')]//li/a/@href").extract()]
            if images is not None:
                item_loader.add_value("images", images)      

            item_loader.add_value("landlord_phone", "06 16 77 86 21")
            item_loader.add_value("landlord_name", "Les Alexiades")
            item_loader.add_value("landlord_email", "contact@lesalexiades.com")


            furnished = response.xpath("//p[@class='data']/span[contains(.,'Meublé')]/following-sibling::span/text()").extract_first()
            if furnished:
                if "NON" in furnished:
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)
            
            elevator = response.xpath("//p[@class='data']/span[contains(.,'Ascenseur')]/following-sibling::span/text()").extract_first()
            if elevator:
                if "NON" in elevator:
                    item_loader.add_value("elevator", False)
                else:
                    item_loader.add_value("elevator", True)
            
            terrace = response.xpath("//p[@class='data']/span[contains(.,'Terrasse')]/following-sibling::span/text()").extract_first()
            if terrace:
                if "NON" in terrace:
                    item_loader.add_value("terrace", False)
                else:
                    item_loader.add_value("terrace", True)

            balcony = response.xpath("//p[@class='data']/span[contains(.,'Balcon')]/following-sibling::span/text()").extract_first()
            if balcony:
                if "NON" in balcony:
                    item_loader.add_value("balcony", False)
                else:
                    item_loader.add_value("balcony", True)

            parking = response.xpath("//p[@class='data']/span[contains(.,'parking')]/following-sibling::span/text()").extract_first()
            if parking:
                if "NON" in parking:
                    item_loader.add_value("parking", False)
                else:
                    item_loader.add_value("parking", True)


            script_data = response.xpath("//script[contains(.,'lng')]//text()").get()
            if script_data:
                lat = script_data.split("lat")[1].split(",")[0].replace(":", "").strip()
                lng = script_data.split("lng")[1].split(",")[0].replace(":", "").replace("}", "").strip()
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)

            yield item_loader.load_item()