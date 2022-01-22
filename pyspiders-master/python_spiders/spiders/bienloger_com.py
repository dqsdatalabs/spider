# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'bienloger_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.bienloger.com/recherche.asp?r_genre=1&r_nature=3&liste_natures=&r_nbpieces=&r_budjet_min=&r_budjet_max=&ville=&cp=&r_ville=&rayon=&tri=&centregestion=&numcentre=&act=rech",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.bienloger.com/recherche.asp?r_genre=1&r_nature=4&liste_natures=&r_nbpieces=&r_budjet_min=&r_budjet_max=&ville=&cp=&r_ville=&rayon=&tri=&centregestion=&numcentre=&act=rech",
                    "http://www.bienloger.com/recherche.asp?r_genre=1&r_nature=104&liste_natures=&r_nbpieces=&r_budjet_min=&r_budjet_max=&ville=&cp=&r_ville=&rayon=&tri=&centregestion=&numcentre=&act=rech",    
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='a_photo']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("?")[0] + f"?recherche_tri=prix_asc&numpage={page}"
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_studio = "".join(response.xpath("//h1/text()").getall())
        if prop_studio and "studio" in prop_studio.lower(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta["property_type"])

        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("-")[-1].split(".")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bienloger_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()", input_type="M_XPATH", split_list={"(":1,")":0})
        
        city = "".join(response.xpath("//h1//text()").getall())
        if city:
            city = city.split("(")[0].strip()
            if "/" in city:
                city = city.split("/")[0]
            elif "-" in city:
                city = city.split(" ")[0]
            elif "PONT A MOUSSON" in city:
                city = "PONT A MOUSSON"
            if "DUPLEX" not in city:
                item_loader.add_value("city", city)
        
        address = response.xpath("//script[@type='text/javascript']/text()").re(r'address = "(.*)"')
        if address:
            item_loader.add_value('address', address)
            
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@id,'annonce_description')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@id,'annonce_description')]//div[contains(.,'Surface')]//text()[1]", input_type="F_XPATH", get_num=True, split_list={":":1,"m":0,",":0})
        
        room_count = response.xpath("//div[contains(@id,'annonce_description')]//div[contains(.,'pièces')]//text()[2]").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            if int(room_count)!=0:
                item_loader.add_value("room_count", room_count)

        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'detail_top_txt tarif')]//text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        if item_loader.get_collected_values("rent")[0]:
            price = item_loader.get_collected_values("rent")[0]
            if int(price)>40000:
                return
        
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@id,'slider')]//li//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[contains(@class,'small')][contains(.,'Charge')]//text()", input_type="M_XPATH", get_num=True, split_list={"Charges:":1,"€":0})
        
        energy_label = response.xpath("//div[contains(@class,'valeur')]//following-sibling::div[contains(@class,'dpe')]//text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)
       
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@id,'detail_agence')]//strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@id,'detail_agence')]//span[contains(@class,'tel_telephone')]//text()", input_type="M_XPATH", replace_list={"\u00a0":""})        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@bienloger.com", input_type="VALUE")
        yield item_loader.load_item()