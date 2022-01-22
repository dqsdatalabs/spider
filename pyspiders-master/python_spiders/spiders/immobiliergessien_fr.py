# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from python_spiders.helper import ItemClear
import json
import re

class MySpider(Spider):
    name = 'immobiliergessien_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
      "PROXY_ON": True,   
    }
    def start_requests(self):
        url = "https://www.immobiliergessien.fr/fr/locations"
        headers = {
            'accept': 'text/html, */*; q=0.01',
            "accept-encoding" : "gzip, deflate, br",
            "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
        }
        start_urls = [
            {
                "formdata" : {
                    'location_search[typeBien][]': '1',
                    "location_search[tri]": "loyerCcTtcMensuel|asc",
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'location_search[typeBien][]': '2',
                    "location_search[tri]": "loyerCcTtcMensuel|asc",
                    },
                "property_type" : "house",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], headers=headers, dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})


    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'minifiche_liste')]//div[@class='photo']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url,
                          callback=self.populate_item,
                          meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.immobiliergessien.fr/fr/locations/{page}"
            yield Request(url,
                          callback=self.parse,
                          meta={'property_type': response.meta.get('property_type'), "page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immobiliergessien_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='prix']/span/text()[1]", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[@class='critere'][contains(.,'habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "m":0, ".":0})

        if response.xpath("//span[@class='critere'][contains(.,'Chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='critere'][contains(.,'Chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        elif response.xpath("//span[@class='critere'][contains(.,'pièce')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='critere'][contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
            

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='critere'][contains(.,'Salle')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[@class='critere'][contains(.,'garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[@class='critere'][contains(.,'Terrasse')]/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[@class='critere'][contains(.,'Garage')]/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//article[@class='descriptif']//p//text()[contains(.,'Libre le') or contains(.,'Libre à')]", input_type="F_XPATH", split_list={"Libre":1}, replace_list={"le":"", "à partir du":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[@data-fancybox='gallery']/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//p[@class='ref']/text(),':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'position = [')]/text()", input_type="F_XPATH", split_list={"position = [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'position = [')]/text()", input_type="F_XPATH", split_list={"position = [":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[@class='critere'][contains(.,'Meublé ')]/text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Immobilier Gessien", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span/@data-telephone", input_type="F_XPATH")

        address = response.xpath("//script[contains(.,'address')]/text()").get()
        if address:
            street = address.split('"streetAddress":"')[1].split('"')[0]
            if street:
                item_loader.add_value("address", street)
            city = address.split('"addressLocality":"')[1].split('"')[0]
            if city:
                item_loader.add_value("address", city)
                item_loader.add_value("city", city)
            zipcode = address.split('"postalCode":"')[1].split('"')[0]
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//article[@class='descriptif']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//div[@class='valeur_conso'][contains(.,'*')]/text()").get()
        if energy_label:
            energy_label = energy_label.replace("*","").strip()
            if energy_label:
                item_loader.add_value("energy_label", str(int(float(energy_label))))
        
        floor = response.xpath("//span[@class='critere'][contains(.,'Étage ')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
                
        yield item_loader.load_item()