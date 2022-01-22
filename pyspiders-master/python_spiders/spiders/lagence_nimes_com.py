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
    name = 'lagence_nimes_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        url = "http://www.lagence-nimes.com/votre-recherche/"
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'http://www.lagence-nimes.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://www.lagence-nimes.com/votre-recherche/',
            'Accept-Language': 'tr,en;q=0.9',
        }
        start_urls = [
            {
                "formdata" : {
                    'data[Search][offredem]': '2',
                    'data[Search][idtype][]': '2'
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'data[Search][offredem]': '2',
                    'data[Search][idtype][]': '1'
                    },
                "property_type" : "house",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], headers=headers, dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[contains(.,'voir')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f'http://www.lagence-nimes.com/votre-recherche/{page}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(f"http://www.lagence-nimes.com/votre-recherche/{page}", 
                            callback=self.parse, 
                            headers=headers, 
                            dont_filter=True,
                            meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='col-md-12 p5']//h2//text()").get()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        address = ", ".join(response.xpath("//p[contains(.,'Ville')][1]/span/text() | //p[contains(.,'Quartier')]/span/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        room_count = response.xpath("//p[contains(.,'Nombre de chambre')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[contains(.,'Nombre de pièce')]/span/text()")
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Lagence_Nimes_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h2/span[@itemprop='productID']//text()", input_type="F_XPATH",split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[contains(.,'Code postal')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(.,'Ville')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[contains(.,'Etage')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[contains(.,'Surface habitable')]/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(.,'Nb de salle d')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(.,'Prix du bien')]/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[contains(.,'Dépôt de garantie')]/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[contains(.,'Charges locatives ')]/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p[contains(.,'Terrasse')]/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[contains(.,'Meublé')]/span/text()[.!='Non renseigné']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[contains(.,'Ascenseur')]/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p[contains(.,'Balcon')]/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="L'agence Nimes", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 66 67 83 60", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="laggence@gmail.com", input_type="VALUE")    
      
        yield item_loader.load_item()