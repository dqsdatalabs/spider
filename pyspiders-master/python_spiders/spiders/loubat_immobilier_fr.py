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
    name = 'loubat_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Loubat_Immobilier_PySpider_france'
    def start_requests(self):
        yield Request("https://www.loubat-immobilier.fr/location/1", callback=self.parse)

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//article[@itemprop='itemListElement']//div[@class='titleArticle']"):
            seen = True
            follow_url = response.urljoin(item.xpath("./h2/a/@href").get())
            property_type = item.xpath("./h3/text()").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
                
        if page == 2 or seen:
            yield Request(f"https://www.loubat-immobilier.fr/location/{page}", callback=self.parse, meta={"page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[contains(@class,'titleBien')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li[contains(.,'Ville')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[contains(.,'Ville')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li[contains(.,'Code')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, " ":0, ",":0})
        
        if response.xpath("//li[contains(.,'Nombre de pièce')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Nombre de pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        elif response.xpath("//li[contains(.,'Nombre de chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Nombre de chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé')]/text()[not(contains(.,'NON')) and not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/text()[not(contains(.,'NON')) and not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'garage')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse :')]/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='ref']/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[contains(.,'Loyer')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'garantie')]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={":":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slider_Mdl']//@src", input_type="M_XPATH")
        
        desc = " ".join(response.xpath("//div[@class='offreContent']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        if "disponible à compter du" in desc:
            available_date = desc.split("disponible à compter du")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LOUBAT IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 68 23 10 54", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="agence.loubat.immobilier@orange.fr", input_type="VALUE")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None