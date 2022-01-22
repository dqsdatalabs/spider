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
    name = 'bonduelleimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Bonduelleimmobilier_PySpider_france'
    headers = {
        'authority': 'www.bonduelleimmobilier.com',
        'upgrade-insecure-requests': '1',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': '',
        'accept-language': 'tr,en;q=0.9',
        # 'cookie': '_ga=GA1.2.1608665077.1612861616; _gid=GA1.2.1864309815.1612861616; tartaucitron=u0021analytics=waitu0021recaptcha=wait'
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bonduelleimmobilier.com/result-rent/?property_type=13&asp_active=1&p_asid=8&p_asp_data=1&current_page_id=29&qtranslate_lang=0&filters_changed=0&filters_initial=1&asp_gen%5B%5D=content&termset%5Bproperty_category%5D%5B%5D=37&aspf%5Bprice__4%5D=&aspf%5Barea_total__5%5D=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bonduelleimmobilier.com/result-rent/?property_type=13&asp_active=1&p_asid=8&p_asp_data=1&current_page_id=714&qtranslate_lang=0&filters_changed=0&filters_initial=1&asp_gen%5B%5D=content&termset%5Bproperty_category%5D%5B%5D=45&aspf%5Bprice__4%5D=&aspf%5Barea_total__5%5D=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                self.headers["referer"] = item
                yield Request(item,
                            callback=self.parse,
                            headers=self.headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='vcc_inner']//a[@class='openFull']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//span[@class='title']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price']/b/text()[contains(.,'€')]", input_type="F_XPATH", get_num=True, split_list={":":1, ",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@id='ref']/text()", input_type="F_XPATH")
        
        if response.xpath("//a[contains(.,'pièce')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//a[contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
            
        else:
            if response.xpath("//span[@class='details']//text()[contains(.,'chambre')]").get():
                ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='details']//text()[contains(.,'chambre')]", input_type="F_XPATH", get_num=True, split_list={" ":0})

        title = " ".join(response.xpath("(//div[@class='desc']//span//p//text())[1]").get())
        if title:
            item_loader.add_value("title", title)
        else: 
            title = " ".join(response.xpath("//span[@class='title']/text()").get())
            if title:
                item_loader.add_value("title", title)        
        
        desc = " ".join(response.xpath("//div[@class='desc']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//a[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//a[contains(.,'Terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//a[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//a[contains(.,'Piscine')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//a[contains(.,'Lave-linge')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//a[contains(.,'Meublé')]/text()", input_type="F_XPATH", tf_item=True)
        
        address = response.xpath("//span[@class='title']/text()").get()
        if address:
            address = address.split("–")[0]
            if "APPARTEMENT" not in address:
                item_loader.add_value("address", address)
                item_loader.add_value("city", address)
            else:
                address = address.split("TERRASSE")[1].strip()
                item_loader.add_value("address",address)
                item_loader.add_value("city",address)
        else:
            address = response.xpath("(//div[@class='desc']//span//p//text())[1]").get()
            if address:
                address = address.split("–")[0]
                if "APPARTEMENT" not in address:
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", address)
                else:
                    address = address.split("TERRASSE")[1].strip()
                    item_loader.add_value("address",address)
                    item_loader.add_value("city",address)
        
        square_meters = response.xpath("//span[@class='details']//text()[contains(.,'m2')]").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        floor = response.xpath("//a[contains(.,'étages')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        energy_label = response.xpath("//img[contains(@src,'ENERGETIQUES')]/@src").get()
        if energy_label:
            energy_label = energy_label.split("ENERGETIQUES-")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//figure//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'Lat')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'Lat')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        
        name = response.xpath("//div[@class='rightBroker']/span/text()").get()
        if name and name.strip():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='rightBroker']/span/text()", input_type="F_XPATH")
        else: item_loader.add_value("landlord_name", "Bonduelle Immobilier")
        if response.xpath("//div[@class='rightBroker']/a[contains(@href,'tel')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='rightBroker']/a[contains(@href,'tel')]/text()", input_type="F_XPATH")
        else: item_loader.add_value("landlord_phone", "+33 6 73 91 62 28")

        yield item_loader.load_item()