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
    name = 'dom_immosud_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Dom_Immosud_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://parnasse-immobilier.com/advanced-search/?filter_search_action%5B0%5D=location&filter_search_type%5B0%5D=appartement&advanced_city&nbre-de-pieces&price_low=0&price_max=5000000&wpestate_regular_search_nonce=02e58f8002&_wp_http_referer=%2F%3Ffbclid%3DIwAR3oi7FIwvm6U_IdKc0V2iGbtebrrhvNSV8y6C1_owWfXVRg3jLDt2EG_Ho",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://parnasse-immobilier.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=maison-villa&advanced_city=&nbre-de-pieces=&price_low=0&price_max=5000000&wpestate_regular_search_nonce=02e58f8002&_wp_http_referer=%2F%3Ffbclid%3DIwAR3oi7FIwvm6U_IdKc0V2iGbtebrrhvNSV8y6C1_owWfXVRg3jLDt2EG_Ho",
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

        for item in response.xpath("//div[@id='listing_ajax_container']/div//h4/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_button:
            yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        externalid=response.xpath("//li[contains(.,'Référence')]/following-sibling::li/text()").get()
        if externalid:
            item_loader.add_value("external_id",externalid)
        item_loader.add_value("external_source",self.external_source)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='property_categs']/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='property_categs']/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div/strong[contains(.,'Code')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div/strong[contains(.,'Réf')]/following-sibling::text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/strong[contains(.,'habitable')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        
        # if response.xpath("//div/strong[contains(.,'chambres')]/following-sibling::text()[not(contains(.,'0'))]").get():
        #     ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/strong[contains(.,'chambres')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        if response.xpath("//div/strong[contains(.,'pièces')]/following-sibling::text()[not(contains(.,'0'))]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/strong[contains(.,'pièces')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div/strong[contains(.,'salle')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div/strong[contains(.,'étage')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div/strong[contains(.,'Date')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div/strong[contains(.,'Prix')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ".000":0},replace_list={" ":"", ".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div/strong[contains(.,'Charges')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div/strong[contains(.,'de garantie')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[contains(@class,'listing_detail')]/i[contains(@class,'checkon')]/following-sibling::text()[contains(.,'Meublé')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[contains(@class,'listing_detail')]/i[contains(@class,'checkon')]/following-sibling::text()[contains(.,'Garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(@class,'listing_detail')]/i[contains(@class,'checkon')]/following-sibling::text()[contains(.,'Ascenseur')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'listing_detail')]/i[contains(@class,'checkon')]/following-sibling::text()[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[contains(@class,'listing_detail')]/i[contains(@class,'checkon')]/following-sibling::text()[contains(.,'Terrasse')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[contains(@class,'listing_detail')]/i[contains(@class,'checkon')]/following-sibling::text()[contains(.,'Piscine')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='owl-demo']//@data-bg", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-cur_lat[.!='0']", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-cur_long[.!='0']", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'agent_contanct')]/h4/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="07 67 47 25 49", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@parnasse-immobilier.com", input_type="VALUE")

        desc = " ".join(response.xpath("//div[contains(@id,'description')]/p//text()").getall())
        if desc: 
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        images=[x for x in response.xpath("//div[@class='gallery_wrapper property_header_gallery_wrapper']//div[contains(@class,'image_gallery')]/@data-bg").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//ul[@class='overview_element']//li/sup/preceding-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        available_date=response.xpath("//strong[contains(.,'Disponibilité')]/following-sibling::text()").get()
        if available_date:
            date=available_date.strip()
            item_loader.add_value("available_date",date)
        furnished=response.xpath("//strong[contains(.,'Meublé')]/following-sibling::text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        energy_label = response.xpath("//div/strong[contains(.,'Classe')]/following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        if "garantie" in desc:
            deposit = desc.split("garantie")[1].split("EUR")[0].replace(":","").strip().replace(" ","")
            item_loader.add_value("deposit", deposit)
        depositcheck=item_loader.get_output_value("deposit")
        if not depositcheck:
            deposit=response.xpath("//strong[contains(.,'Dépôt')]/following-sibling::text()").get()
            if deposit:
                item_loader.add_value("deposit",deposit.split("€")[0].strip())

        landlord_name = " ".join(response.xpath("//div[contains(@class,'agent_contanct')]/h4/a/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Parnasse Immobilier")
        
        yield item_loader.load_item()