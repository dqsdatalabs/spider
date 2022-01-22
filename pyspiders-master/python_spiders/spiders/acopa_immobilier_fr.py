# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'acopa_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = ["https://www.acopa-immobilier.fr/wp-admin/admin-ajax.php"]
        payload="action=corn_realestateSearch&nonce=2418b3cd57&param=budget_max=3950&surface_min=0&rooms_min=0&ville=&typetransacselected=location&mapselected=paris&zoneselected=&lastcriteria=prix"
        headers = {
            'authority': 'www.acopa-immobilier.fr',
            'accept': '*/*',
            'x-requested-with': 'XMLHttpRequest',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://www.acopa-immobilier.fr',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.acopa-immobilier.fr/',
            'accept-language': 'tr,en;q=0.9'
        }

        yield Request(start_urls[0], method="POST", callback=self.parse, headers=headers, body=payload)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            property_type = item.xpath(".//h3/text()[1]").get()
            if get_p_type_string(property_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Acopa_Immobilier_PySpider_france", input_type="VALUE")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_css("title", "title")
        
        address = " ".join(response.xpath("//span[@class='adr']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='locality']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@class='postal-code']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'fiche')]/div/div[contains(.,'Réf')]/following-sibling::div[1]/text()", input_type="F_XPATH")
        
        room_count = response.xpath("//div[contains(@class,'fiche')]/div/div[contains(.,'Chambre')]/following-sibling::div[1]/text()").get()
        if room_count:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'fiche')]/div/div[contains(.,'Chambre')]/following-sibling::div[1]/text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'fiche')]/div/div[contains(.,'Pièce')]/following-sibling::div[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(@class,'fiche')]/div/div[contains(.,'SdB')]/following-sibling::div[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'price')]/parent::div/span[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(@class,'surface')]/parent::div/span[1]/text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//img/@src[contains(.,'energie')]").get()
        if energy_label:
            energy_label = energy_label.split("energie_")[1].split(".")[0]
            if "NC" not in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","").replace("e","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'long')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat=")[1].split("&")[0]
            longitude = latitude_longitude.split("long=")[1]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        import dateparser
        if "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p/span[contains(.,'Balcon')]/following-sibling::span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'photo-thb')]//@data-image", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//a[contains(@id,'contactagences')]/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//a[contains(@class,'tel')]/text()", input_type="F_XPATH", replace_list={"+":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="", input_type="VALUE")

            
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None