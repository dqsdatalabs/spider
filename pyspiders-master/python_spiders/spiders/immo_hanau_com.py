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
    name = 'immo_hanau_com'
    execution_type='testing'
    country='france'
    locale='fr'


    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }    


    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immo-hanau.com/a-louer/appartements/1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.immo-hanau.com/a-louer/maisons-villas/1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//span[contains(.,'Voir le bien')]/../@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("/" + str(page - 1), "/" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        
            
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immo_Hanau_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p/span[contains(.,'Code')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="normalize-space(//div[@class='bienTitle']/h2/text())", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p/span[contains(.,'habitable')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        
        if response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'chambre')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'pièce')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p/span[contains(.,'salle')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p/span[contains(.,'Loyer')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/span[contains(.,'Dépôt')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[@class='ref']/text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat :":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p/span[contains(.,'Etage')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p/span[contains(.,'Charges')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p/span[contains(.,'garage') or contains(.,'parking')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p/span[contains(.,'Balcon')]/following-sibling::span/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p/span[contains(.,'Meublé')]/following-sibling::span/text()[not(contains(.,'Non')) and not(contains(.,'NON'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p/span[contains(.,'Ascenseur')]/following-sibling::span/text()[not(contains(.,'Non')) and not(contains(.,'NON'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p/span[contains(.,'Terrasse')]/following-sibling::span/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Immo Hanau", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 88 71 34 62", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="immohanau@wanadoo.fr", input_type="VALUE")

        available_date = "".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if available_date:
            available = ""
            if "disponible le" in available_date.lower():
                available = available_date.lower().split("disponible le")[1].split("\n")[0].strip()
            elif "disponible au" in available_date.lower():
                available = available_date.lower().split("disponible au")[1].split("\n")[0].strip()
            elif "disponible en" in available_date.lower():
                available = available_date.lower().split("disponible en")[1].split("\n")[0].strip()
            import dateparser
            if available:
                date_parsed = dateparser.parse(available, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        yield item_loader.load_item()