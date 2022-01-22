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
    name = 'avocette_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.avocette.fr/ajax/ListeBien.php?page={}&TypeModeListeForm=text&ope=2&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.avocette.fr/ajax/ListeBien.php?page={}&TypeModeListeForm=text&ope=2&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        total_page = response.xpath("//span[@class='nav-page-position']/text()").get()
        if total_page:
            total_page = int(total_page.split("/")[1].strip())
        else:
            total_page = 1
        for item in response.xpath("//a[@itemprop='url']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page <= total_page:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Avocette_PySpider_france", input_type="VALUE")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        title = " ".join(response.xpath("//h1[@class='detail-bien-type']/text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[contains(@class,'ville')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[contains(@class,'ville')]//text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[contains(@class,'ville')]//text()", input_type="F_XPATH",split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@itemprop='productID']//text()", input_type="M_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//ul[@class='nolist']/li[contains(.,'m²')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        if response.xpath("//ul[@class='nolist']/li[contains(.,'chambre') and not(contains(.,'NC'))]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul[@class='nolist']/li[contains(.,'chambre')]//text()[not(contains(.,'NC'))][normalize-space()]", input_type="M_XPATH", get_num=True, split_list={"ch":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul[@class='nolist']/li[contains(.,'pièce')]//text()", input_type="M_XPATH", get_num=True, split_list={"p":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//ul[@class='nolist']/li[contains(.,'de garantie')]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//ul[@class='nolist']/li[contains(.,'charges')]//span[2]/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'bien-prix')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        desc = " ".join(response.xpath("//span[@itemprop='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        from datetime import datetime
        import dateparser
        if "disponible" in desc.lower():
            available_date = desc.lower().split("disponible")[1].strip()
            if "immédiatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "partir du" in available_date:
                available_date = available_date.split("partir du")[1].split(".")[0]
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        if "salle" in desc:
            bathroom_count = desc.split("salle")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        energy_label = response.xpath("//img/@src[contains(.,'nrj')]").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].split(".")[0]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label)
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//img[contains(@class,'slideshow ')]//@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//li[contains(@class,'lat')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//li[contains(@class,'lng')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AVOCETTE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@id='contact-nego-coord']/span/span[contains(.,'Tel.')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@id='contact-nego-coord']/span/span[contains(.,'E-mail')]/following-sibling::text()", input_type="F_XPATH")

        yield item_loader.load_item()