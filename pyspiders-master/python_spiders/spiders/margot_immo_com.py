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
    name = 'margot_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.margot-immo.com/recherche,incl_recherche_basic_ajax.htm?idpays=250&surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&idqfix=1&idtt=1&pres=basic&lang=fr&idtypebien=1&tri=d_dt_crea&_=1613725414468&ANNLISTEpg={}",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Margot_Immo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'span6')]//div[contains(@class,'bloc-detail-reference')]/span//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="M_XPATH", split_list={"-":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()", input_type="M_XPATH", split_list={"-":1,"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()", input_type="M_XPATH",split_list={"-":1,"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'description')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(@title,'Surface')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True, split_list={"m²":0,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(.,'Loyer')]//text()", input_type="M_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        if response.xpath("//li[contains(@title,'Chambres')]//div[contains(@class,'bold')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@title,'Chambres')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li[contains(@title,'Pièce')]//div[contains(@class,'bold')]//text()").getall():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@title,'Pièce')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@title,'Salle')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(@title,'Etage')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//img[contains(@rel,'gal')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'garantie')]//text()[not(contains(.,'N/A'))]", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(@title,'Balcon')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(@title,'Ascenseur')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(@title,'Meublé')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(@title,'Terrasse')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@id,'detail-agence-nom')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'bloc-detail-adresse-agence')]//span[contains(@id,'numero-telephonez-nous-detail')]//text()", input_type="M_XPATH")
        
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        energy_label = response.xpath("//div[contains(@class,'dpe-bloc-lettre')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p[contains(@itemprop,'description')]/text()").getall())
        if available_date and "disponible" in available_date.lower():
            available_date = available_date.lower().split("disponible")[1]
            if "/" in available_date:
                available_date = available_date.replace("le","").strip()
            else:
                available_date = available_date.replace("au","").replace(".","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        yield item_loader.load_item()