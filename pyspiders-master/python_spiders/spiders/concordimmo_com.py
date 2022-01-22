# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
import dateparser
class MySpider(Spider):
    name = 'concordimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.concordimmo.com/a-louer/appartements/1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.concordimmo.com/a-louer/maisons/1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.concordimmo.com/a-louer/studios/1",
                ],
                "property_type" : "studio"
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

        for item in response.xpath("//a[@class='link-bien']/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("/" + str(page - 1), "/" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        room_count = response.xpath("//li[text()='Nombre de chambre(s) : ']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)    
        else:
            room_count = response.xpath("//li[text()='Nombre de pièces : ']/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)  
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='ref']//text()", input_type="F_XPATH",split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Concordimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//section[contains(@class,'map-infos-city')]//h1//text()", input_type="F_XPATH", split_list={'ville de':-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li[text()='Code postal : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[text()='Ville : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='infos']//h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[text()='Etage : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[h2[contains(.,'Détails')]]/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[text()='Surface habitable (m²) : ']/span/text()", input_type="F_XPATH", get_num=True, split_list={',':0, "m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Nb de salle d')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[text()='Loyer CC* / mois : ']/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[text()='Dépôt de garantie TTC : ']/span/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""}, split_list={',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='glider']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges locatives')]/span/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""}, split_list={',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[text()='Meublé : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[text()='Balcon : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[text()='Ascenseur : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[text()='Terrasse : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[text()='Terrain piscinable : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[text()='Nombre de garage : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CONCORD'IMMO", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 89 56 58 62", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="accueil.concordimmo@gmail.com", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1,"}":0})
        a_date = response.xpath("//div[h2[contains(.,'Détails')]]/p//text()[contains(.,'Disponible le')]").extract_first()
        if a_date:
            re_date = re.search(r'(\d+/\d+/\d+)',a_date)
            date_parsed = dateparser.parse(re_date.group(1), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        yield item_loader.load_item()