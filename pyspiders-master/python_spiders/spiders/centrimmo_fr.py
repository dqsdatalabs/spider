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
import dateparser
class MySpider(Spider):
    name = 'centrimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.centrimmo.fr/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.centrimmo.fr/recherche?a=2&b%5B%5D=house&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='res_tbl1']"):
            status = item.xpath("./div/@data-rel").get()
            if status and "loue" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Centrimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={".htm":0, "_":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//tr[td[.='Ville']]/td[2]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//tr[td[.='Ville']]/td[2]//span[@class='acc']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//tr[td[.='Ville']]/td[2]//span[@itemprop='addressLocality']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//tr[td[.='Étage']]/td[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='dpe-container'][div[contains(.,'énergétiques')]]//b[@class='dpe-letter-active']/text()", input_type="F_XPATH", split_list={":":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='details']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//tr[td[.='Surface']]/td[2]/text()", input_type="F_XPATH", get_num=True,split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//tr[td[contains(.,'Salle d')]]/td[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//tr/td[@itemprop='price']/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='basic_copro']//text()[contains(.,'Dépôt de garantie')]", input_type="F_XPATH", get_num=True, split_list={"Dépôt de garantie":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//tr[td[.='Charges']]/td[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='layerslider']/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//tr[td[.='Piscine']]/td[2]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//tr[td[.='Ascenseur']]/td[2]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//tr[td[.='Balcon']]/td[2]//text()[contains(.,'Oui') or contains(.,'Non')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='mandataires']/strong[@class='info_name']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='mandataires']/span[@itemprop='telephone']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@class='mandataires']/span[@itemprop='email']/script/text()", input_type="F_XPATH", split_list={"('":1,"')":0},replace_list={"', '":"@"} )
        available_date = response.xpath("//tr[td[.='Disponibilité']]/td[2]//text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        room = response.xpath("//tr[td[.='Chambres']]/td[2]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room)
        else:
            room = response.xpath("//tr[td[.='Pièces']]/td[2]/text()").extract_first()
            if room:
                item_loader.add_value("room_count", room)
        furnished = response.xpath("//tr[td[.='Ameublement']]/td[2]/text()").extract_first()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        parking = response.xpath("//tr[td[contains(.,'Stationnement int.')]]/td[2]/text()").extract_first()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        yield item_loader.load_item()