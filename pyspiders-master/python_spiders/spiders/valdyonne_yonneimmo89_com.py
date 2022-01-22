# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'valdyonne_yonneimmo89_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    start_urls = 'https://valdyonne.yonneimmo89.com/fr/locations'  # LEVEL 1

    custom_settings = {
        "PROXY_ON": True,      
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307]       
    }

    headers = {
        "accept": "text/html, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "origin": "https://valdyonne.yonneimmo89.com",
        "referer": "https://valdyonne.yonneimmo89.com/fr/locations",
    }
    
    form_data = {
        "location_search[pTypeBien][]": "",
        "location_search[loyer_min]": "0",
        "location_search[loyer_max]": "1000000",
        "location_search[rayonCommune]": "4",
        "location_search[typeBien][]": "", 
    }
    
    def start_requests(self):
        start_urls = [
            {  
                "type": "1",
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            self.form_data["location_search[pTypeBien][]"] = url.get('type')
            self.form_data["location_search[typeBien][]"] = url.get('type')
            yield FormRequest(
                url=self.start_urls,
                formdata= self.form_data,
                headers=self.headers,
                callback=self.parse,
                meta={'property_type': url.get('property_type')}
            )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='btn_fiches']/a/@href[contains(.,'location')]").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Valdyonne_Yonneimmo89_PySpider_france")
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//div[@class='btn_detail']/h3//text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        
        city = response.xpath("//div[@class='btn_detail']/h3//text()[2]").get()
        if city:
            item_loader.add_value("city", city.split(" - ")[1].strip())
            item_loader.add_value("zipcode", city.split(" - ")[0].strip())
        
        square_meters = response.xpath("//li[contains(.,'habitable')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        deposit = response.xpath("//li[contains(.,'Dépôt')]/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        bathroom_count = response.xpath("//li[contains(.,'salle')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = "".join(response.xpath("//i[contains(@class,'prix')]/following-sibling::text()").getall())
        if rent:
            price = rent.split("€")[0].strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
            
        furnished = response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished", True)

        floor = "".join(response.xpath("//i[contains(@class,'etage')]/following-sibling::text()").getall())
        if floor and floor.strip():
            item_loader.add_value("floor", floor.strip())

        parking = "".join(response.xpath("//i[contains(@class,'garage')]/following-sibling::text()").getall())
        if parking and parking.strip():
            if "0" not in parking:
                item_loader.add_value("parking", True)

        external_id = response.xpath("substring-after(//p[contains(.,'Réf')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        utilities = response.xpath("//hr[contains(@class,'charge')]/following-sibling::div//text()[contains(.,'charges')]").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("utilities", utilities)

        description = " ".join(response.xpath("//article[@class='description-immo']//p//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in description.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        room_count = "".join(response.xpath("//i[contains(@class,'chambre')]/following-sibling::text()").getall())
        if room_count and room_count.strip():
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        elif "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        
        images = [x for x in response.xpath("//span[contains(@class,'imgDetail')]/@data-fancybox-href").getall()]
        if images:
            item_loader.add_value("images", images)

        energy_label = response.xpath("//div[@class='valeur_conso']//text()[contains(.,'*')]").get()
        if energy_label:
            energy_label = energy_label.split("*")[0].strip()
            item_loader.add_value("energy_label", energy_label)
        
        latitude_longitude = response.xpath("//script[contains(.,'position =')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('position = [')[1].split(',')[0]
            longitude = latitude_longitude.split('position = [')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Val d'Yonne Immobilier")
        item_loader.add_value("landlord_phone", "03 86 89 00 30")
        
        yield item_loader.load_item()
