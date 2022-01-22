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
    name = 'houbron_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        yield Request("http://www.houbron-immobilier.com/location,0.html", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='information_produit']"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Plus de détails')]/@href").get())
            property_type = item.xpath("./h4/text()").get()
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_page = response.xpath("//a[contains(.,'>')]/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Houbron_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'Réf')]//text()", input_type="M_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//span[contains(@itemprop,'name')]//text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//span[contains(@itemprop,'name')]//text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//span[contains(@class,'description')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'m²')]//text()", input_type="M_XPATH", get_num=True, split_list={"environ":1,"m":0,".":0})
        if "".join(response.xpath("//li[contains(.,'chambre')]//text()").getall()):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'chambre')]//text()", input_type="M_XPATH", get_num=True, split_list={" ":-1})
        elif "".join(response.xpath("//li[contains(.,'pièce')]//text()").getall()):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'pièce')]//text()", input_type="M_XPATH", get_num=True, split_list={" ":-1})
        # ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(.,'Loyer CC')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[contains(.,'Dépôt de garantie TTC')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0})
        
        utilities = response.xpath("//p[contains(.,'TTC de charge')]//text()").get()
        if utilities and "n.c." not in utilities:
            utilities = utilities.split("Dont")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        energy_label = response.xpath("//img[contains(@id,'img_dpe')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("value=")[1].split("&")[0]
            if energy_label != "0":
                item_loader.add_value("energy_label", energy_label)

        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'carousel-inner')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="HOUBRON IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 0 4 75 04 01 90", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@houbron-immobilier.net", input_type="VALUE")

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