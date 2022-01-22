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
    name = 'agencelacroix_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        yield Request("http://agencelacroix.fr/a-louer/1", callback=self.parse)

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//ul[@class='listingUL']/li"):
            seen = True
            property_type = item.xpath(".//h2/text()").get()
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

        if page == 2 or seen:
            yield Request(f"http://agencelacroix.fr/a-louer/{page}", callback=self.parse, meta={"page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        title = " ".join(response.xpath("//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agencelacroix_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p/span[contains(.,'Ville')]/../span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p/span[contains(.,'Code')]/../span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p/span[contains(.,'Ville')]/../span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p/span[contains(.,'habitable')]/../span[2]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
            
        if response.xpath("//p/span[contains(.,'chambre')]/../span[2]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'chambre')]/../span[2]/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//p/span[contains(.,'pièce')]/../span[2]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'pièce')]/../span[2]/text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p/span[contains(.,'salle')]/../span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p/span[contains(.,'Loyer')]/../span[2]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/span[contains(.,'garantie')]/../span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref']/text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p/span[contains(.,'Etage')]/../span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p/span[contains(.,'Charges')]/../span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p/span[contains(.,'Balcon')]/../span[2]/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p/span[contains(.,'Meublé')]/../span[2]/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p/span[contains(.,'Ascenseur')]/../span[2]/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p/span[contains(.,'Terrasse')]/../span[2]/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LACROIX GENTILLY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 46 63 56 66", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="gentilly@agencelacroix.fr", input_type="VALUE")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None