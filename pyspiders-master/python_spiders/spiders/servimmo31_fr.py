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
    name = 'servimmo31_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.servimmo31.fr/fr/liste.htm?ope=2&filtre=2",
                "property_type": "apartment",
                "type":"2",
            },
	        {
                "url": "https://www.servimmo31.fr/fr/liste.htm?ope=2&filtre=8",
                "property_type": "house",
                "type":"8",
            
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'liste-bien-container')]"):
            property_type = item.xpath(".//h2[@class='liste-bien-type']/text()").get()
            if "Parking" in property_type or "Bureau" in property_type:
                return
            follow_url = response.urljoin(item.xpath("./div[@class='liste-bien-photo']//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})
            seen = True
        
        next_page = response.xpath("//span[@class='nav-page-position']/text()").get()
        if next_page:
            prop_type = response.meta.get('type')
            next_page = next_page.split("/")[-1].strip()
            for i in range(1,int(next_page)+1):
                url = f"https://www.servimmo31.fr/fr/liste.htm?page={i}&ListeViewBienForm=text&ope=2&filtre={prop_type}"
                yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        external_id = "+".join(response.xpath("//span[contains(.,'Ref')]//following-sibling::span[contains(@itemprop,'productID')]//text()").getall())
        if external_id:
            external_id = external_id.split("+")[-1].strip()
            item_loader.add_value("external_id", external_id)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Servimmo31_PySpider_france", input_type="VALUE")

        title = " ".join(response.xpath("//div[contains(@class,'detail-bien-type')]/text() | //h2[contains(@class,'ville')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[contains(@class,'ville')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[contains(@class,'ville')]//text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[contains(@class,'ville')]//text()", input_type="F_XPATH",split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//span[contains(@itemprop,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(@class,'ico-surface')]//parent::li/text()", input_type="F_XPATH", get_num=True, split_list={"m":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'detail-bien-prix')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'charges_mens')][contains(.,'garantie')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True,split_list={".":0})
        if response.xpath("//span[contains(@class,'ico-chambre')]//parent::li/text()[not(contains(.,'NC'))]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(@class,'ico-chambre')]//parent::li/text()", input_type="F_XPATH", get_num=True,split_list={" ":0})
        elif response.xpath("//span[contains(@class,'ico-piece')]//parent::li/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(@class,'ico-piece')]//parent::li/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'thumbs-flap-container')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'charges_mens')][contains(.,'charges')]//following-sibling::span[contains(@class,'cout_charges_mens')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[contains(@class,'DpeNote')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//li[contains(@class,'gg-map-marker-lat')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//li[contains(@class,'gg-map-marker-lng')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="SERVIMMO GESTION", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 61 78 28 44", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@servimmo31.fr", input_type="VALUE")

        yield item_loader.load_item()