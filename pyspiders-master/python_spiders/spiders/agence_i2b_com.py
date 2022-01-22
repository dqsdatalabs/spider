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
    name = 'agence_i2b_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence-i2b.com/resultats/?wpestate_adv_search_nonce_field=7f512594ac&_wp_http_referer=%2F&lang=fr&adv_location=&filter_search_type%5B%5D=appartement&filter_search_action%5B%5D=a-louer&is2=1&submit=CHERCHER",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agence-i2b.com/resultats/?wpestate_adv_search_nonce_field=7f512594ac&_wp_http_referer=%2Fresultats%2F%3Fwpestate_adv_search_nonce_field%3D7f512594ac%26_wp_http_referer%3D%2F%26adv_location%3D%26filter_search_type%5B0%5D%3Dappartement%26filter_search_action%5B0%5D%3Da-louer%26is2%3D1%26submit%3DCHERCHER&lang=fr&adv_location=&filter_search_type%5B%5D=maison&filter_search_action%5B%5D=a-louer&is2=1&submit=CHERCHER",
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
        for item in response.xpath("//div[@class='listing-unit-img-wrapper']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_I2b_PySpider_france", input_type="VALUE")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[contains(@class,'title')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'price_area')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(@class,'adres_area')]//a/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div/strong[contains(.,'Ville')]/following-sibling::a/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div/strong[contains(.,'Code')]/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div/strong[contains(.,'Référence')]/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/strong[contains(.,'Habitable')]/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/strong[contains(.,'Chambr')]/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div/strong[contains(.,'Parking')]/following-sibling::text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div/strong[contains(.,'Ascenseur')]/following-sibling::text()[contains(.,'oui') or contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-cur_lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-cur_long", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@id='property_description']//text()[contains(.,'de garantie')]", input_type="F_XPATH", split_list={":":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@id='property_description']//text()[contains(.,'euros de provisions')]", input_type="F_XPATH", split_list={"euros":1, " ":-1})
        
        desc = " ".join(response.xpath("//div[@id='property_description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='owl-demo']//@src", input_type="M_XPATH")
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AGENCE I2B", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="06 25 69 03 21", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@agence-i2b.com", input_type="VALUE")
        
        yield item_loader.load_item()