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
    name = 'parnasse_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://parnasse-immobilier.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=appartement&advanced_city=&nbre-de-pieces=&price_low=0&price_max=5000000&wpestate_regular_search_nonce=b4a78783ee&_wp_http_referer=%2F",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://parnasse-immobilier.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=maison-villa&advanced_city=&nbre-de-pieces=&price_low=0&price_max=5000000&wpestate_regular_search_nonce=b4a78783ee&_wp_http_referer=%2F",
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

        for item in response.xpath("//div[contains(@class,'property_listing ')]/h4/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Parnasse_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@id,'propertyid')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//strong[contains(.,'Ville')]//following-sibling::a//text() | //strong[contains(.,'Code')]//parent::div/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//strong[contains(.,'Code')]//parent::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//strong[contains(.,'Ville')]//following-sibling::a//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//strong[contains(.,'Surface')]/parent::div/text()", input_type="M_XPATH", get_num=True, split_list={"m":0})
        if response.xpath("//strong[contains(.,'Chambre')]/parent::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//strong[contains(.,'Chambre')]/parent::div/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//strong[contains(.,'pièces')]/parent::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//strong[contains(.,'pièces')]/parent::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//strong[contains(.,'salle')]/parent::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//strong[contains(.,'Prix')]/parent::div/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'owl-demo')]//@data-bg", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//strong[contains(.,'Classe énergétique')]/parent::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//strong[contains(.,'étage')]/parent::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//strong[contains(.,'Charges')]/parent::div/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//strong[contains(.,'Parking')]/parent::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//strong[contains(.,'Balcon')]/parent::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[contains(@class,'listing_detail')]//text()[contains(.,'Meublé')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(@class,'listing_detail')]//text()[contains(.,'Ascenseur')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[contains(@class,'listing_detail')]//text()[contains(.,'Terrasse')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Parnasse Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="07 67 47 25 49", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@parnasse-immobilier.com", input_type="VALUE")
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//strong[contains(.,'disponible')]/parent::div/text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        yield item_loader.load_item()