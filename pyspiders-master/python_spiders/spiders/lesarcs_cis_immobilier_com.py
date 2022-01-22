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
    name = 'lesarcs_cis_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cis-immobilier.com/search/tax-transactions/location/type_goods/1/page/1/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cis-immobilier.com/search/tax-transactions/location/type_goods/2/page/1/",
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

        for item in response.xpath("//article[contains(@id,'post-')]/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = ""
            if "page/" in response.url:
                follow_url = response.url.replace("page/" + str(page - 1), "page/" + str(page))
            else:
                follow_url = response.url + f"page/{page}/"
            yield Request(follow_url, callback=self.parse, meta={"page": page + 1, "property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Lesarcs_Cis_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={"m²":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={"m²":1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'descr-text')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span/sup/parent::span/text()", input_type="F_XPATH", get_num=True, split_list={"M":0})
        
        if response.xpath("//span[contains(.,'chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        elif response.xpath("//span[contains(.,'pièce')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(.,'Loyer')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//div[contains(@class,'ref')]//text(),'Réf.')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'galerie')]//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(.,'charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'garage') or contains(.,'parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[contains(@class,'descr-text')]//p//text()[contains(.,'étage')]", input_type="F_XPATH", split_list={"étage":0, " ":-1}, replace_list={"deuxième":"", "dernier":"", "premier":"","L’":"", "°":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CIS Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 79 25 01 97", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="bourgetdulac@cis-immobilier.com", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//span[@class='nq-c-AgenceItem-mail']//a[contains(@href,'mailto:')]/text()", input_type="F_XPATH")

        energy_label = response.xpath("//span[contains(@class,'etiquette-flag dpe')]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[-1])
        
        import dateparser
        available_date = response.xpath("//div[contains(@class,'descr-text')]//p//text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.split("Disponible")[1].replace("–",".").split(".")[0].lower().replace("le ","").replace("début","")
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if not item_loader.get_collected_values("images"):
            images = [response.urljoin(x) for x in response.xpath("//a[@data-fancybox='images']/@href").getall()]
            if images: item_loader.add_value("images", images)
        
        yield item_loader.load_item()