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
    name = 'grauduroi_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.grauduroi.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&lang=fr&idtypebien=1&tri=d_dt_crea&_=1613640101271&annlistepg=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.grauduroi.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&lang=fr&idtypebien=2&tri=d_dt_crea&_=1613640101277&annlistepg=1",
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
        max_page = response.xpath("//div[@id='recherche-resultats-listing']/following-sibling::div/ul[@class='hidden-phone']/li[last()]/a/text()").get()
        max_page = int(max_page) if max_page else -1

        for item in response.xpath("//div[@id='recherche-resultats-listing']//div[@class='span8']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page <= max_page:
            follow_url = response.url.replace("&annlistepg=" + str(page - 1), "&annlistepg=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={'property_type': response.meta["property_type"], 'page': page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Grauduroi_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Référence')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="M_XPATH", split_list={"-":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()", input_type="M_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()", input_type="M_XPATH", split_list={"-":-1,"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(@title,'Surface')]//div[contains(.,'m²')]//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        if response.xpath("//li[contains(@title,'Chambres')]//div[contains(@class,'bold')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@title,'Chambres')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li[contains(@title,'Pièce')]//div[contains(@class,'bold')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@title,'Pièce')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@title,'Salle')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'prix')]//span[contains(@itemprop,'price')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[contains(@class,'charges')][contains(.,'garantie')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[contains(@class,'gallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[contains(@class,'dpe-bloc-lettre')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(@title,'Etage')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Provisions pour charges')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(@title,'Parking')]//div[contains(@class,'bold')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(@title,'Meublé')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(@title,'Ascenseur')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(@title,'Terrasse')]//div[contains(@class,'bold')]//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@id,'detail-agence-nom')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@itemtype,'RealEstateAgent')]//span[contains(@id,'numero-telephonez-nous-detail')]//text()", input_type="M_XPATH") 
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//span[@id='emailAgence']/text()", input_type="F_XPATH")       
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'AGLATITUDE')]/text()", input_type="F_XPATH", split_list={'AGLATITUDE: "':1, '"':0})     
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'AGLATITUDE')]/text()", input_type="F_XPATH", split_list={'AGLONGITUDE: "':1, '"':0})         

        if not item_loader.get_collected_values("utilities"):
            ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges forfaitaires')]/text()", input_type="F_XPATH", get_num=True)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        yield item_loader.load_item()