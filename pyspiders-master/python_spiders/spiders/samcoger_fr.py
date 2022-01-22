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
class MySpider(Spider):
    name = 'samcoger_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.samcoger.fr/moteur,prevalidation.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%C3%B9+d%C3%A9sirez-vous+habiter+%3F&idq=&div=&idpays=&cp=&ci=&tri=d_dt_crea&annlistepg={}",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item.format(1),
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "base":item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='span8']/a"):
            seen = True
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
    
        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type'), "page":page+1, "base":base}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)
        address = response.xpath("//h1//text()[last()]").get()
        if address:
            item_loader.add_value("address", address)   
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip()) 
        room_count = response.xpath("//li[div[.='Chambres' or .='Chambre']]/div[2]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[div[.='Pièces' or .='Pièce']]/div[2]/text()")

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value='//li[div[.="Salle de bain" or .="Salle d\'eau"]]/div[2]/text()', input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Samcoger_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='bloc-detail-reference']/span/text()[contains(.,'Référence')]", input_type="F_XPATH",split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[div[.='Etage']]/div[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[p='Consommations énergétiques']//div[@class='row-fluid']/div[contains(@class,'dpe-bloc-lettre')]/text()[not(contains(.,'V'))]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[div[.='Surface']]/div[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'recherche-annonces-prix')]//span[@itemprop='price']/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'Dépôt de garantie :')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges :')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[div[.='Parking']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[div[.='Meublé']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[div[.='Ascenseur']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[div[.='Balcon']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,',LATITUDE: \"')]/text()", input_type="F_XPATH", split_list={',LATITUDE: "':2,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,',LATITUDE: \"')]/text()", input_type="F_XPATH", split_list={',LONGITUDE: "':2,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="SAMCOGER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 42 36 15 85", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@samcoger.fr", input_type="VALUE")
      
        yield item_loader.load_item()