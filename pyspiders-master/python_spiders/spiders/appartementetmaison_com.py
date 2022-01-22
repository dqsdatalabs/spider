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
    name = 'appartementetmaison_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    def start_requests(self):
        url = "https://www.appartementetmaison.com/a-louer/1"
        yield Request(url, callback=self.parse)  

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//meta[@itemprop='url']"):
            follow_url = response.urljoin(item.xpath("./@content").get())
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Appartementetmaison_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//span[@class='ref']/text(),'Ref')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='themTitle']/h1[@itemprop='name']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@id='dataContent']//p[span[.='Ville']]/span[2]/text() ", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@id='dataContent']//p[span[.='Code postal']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@id='dataContent']//p[span[.='Ville']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@id='dataContent']//p[span[contains(.,'Etage')]]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@id='dataContent']//p[span[contains(.,'Surface habitable')]]/span[2]/text()", input_type="F_XPATH", get_num=True , split_list={",":0} )
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@id='dataContent']//p[span[contains(.,'Nombre de chambre')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='dataContent']//p[span[contains(.,'Nb de salle d')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent_string", input_value="//div[@id='dataContent']//p[span[contains(.,'Loyer CC')]]/span[2]/text()", input_type="F_XPATH",replace_list={" ":""} )
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@id='dataContent']//p[span[contains(.,'Dépôt de garantie')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@id='dataContent']//p[span[contains(.,'Charges ')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]/li/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@id='dataContent']//p[span[contains(.,'Meublé')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='dataContent']//p[span[contains(.,'Terrasse')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="L'EXPERT IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0660815296", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lexpertimmobilier@sfr.fr", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'getMapBien')]/text()", input_type="F_XPATH", split_list={"lat :": 1, ",": 0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'getMapBien')]/text()", input_type="F_XPATH", split_list={"lng:": 1, "}": 0})
        
        yield item_loader.load_item()

