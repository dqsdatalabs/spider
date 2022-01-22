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
from python_spiders.helper import ItemClear


class MySpider(Spider):
    name = 'egea_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.egea-immobilier.com/catalog/advanced_search_result_carto.php?action=update_search&search_id=1689471108108727&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_REPLACE=1&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_30_MIN=0&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.egea-immobilier.com/catalog/advanced_search_result_carto.php?action=update_search&search_id=1689471108108727&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_REPLACE=2&C_27_REPLACE=17&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2%2C17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_30_MIN=0&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='titreBien']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Egea_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@id='detailHidden']//div[.='Etage']/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'loyer_price')][contains(.,'Loyer')]/text()", get_num=True, input_type="F_XPATH", split_list={"€":0, "Loyer":1}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'text-center')][contains(.,'m²')]//text()", input_type="F_XPATH", split_list={"m²":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@id='detailHidden']//div[.='Chambres']/following-sibling::div//text()", input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='detailHidden']//div[.='Salle(s) de bains']/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'depot')][contains(.,'de garantie')]//text()", get_num=True, input_type="F_XPATH", split_list={"€":0, ":":1}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'hono')][contains(.,'Hono')]//text()", get_num=True, input_type="F_XPATH", split_list={"€":0, ":":1}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img[contains(@src,'DPE_')]/@src", get_num=True, input_type="F_XPATH", split_list={"DPE_":1, "_":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})

        room_count = response.xpath("//h1[@class='text-uppercase']/text()").re_first(r'(\d)\schambre')
        if room_count:
            item_loader.add_value('room_count', room_count)
        address = response.xpath('//span[@class="alur_location_ville"]/text()').get()
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('zipcode', re.sub(r'\D', '', address))
            item_loader.add_value('city', re.sub(r'\d', '', address))

        desc = " ".join(response.xpath("//div[contains(@class,'description')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [response.urljoin(x.split("url('..")[1].split("')")[0]) for x in response.xpath("//div[@id='diapoDetail']//div/@style[contains(.,'image')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//div[@id='detailHidden']//div[.='Meublé']/following-sibling::div//text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div[@id='detailHidden']//div[.='Ascenseur']/following-sibling::div//text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div[@id='detailHidden']//div[contains(.,'terrasse')]/following-sibling::div//text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="EGEA IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 61 69 30 30", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="mazeres@egea-immobilier.com", input_type="VALUE")

        
        yield item_loader.load_item()