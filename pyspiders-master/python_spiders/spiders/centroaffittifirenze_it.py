# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'centroaffittifirenze_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Centroaffittifirenze_PySpider_italy"

    def start_requests(self):

        start_urls = [
            {
                "url" : "http://centroaffittifirenze.it/it/Affitti/",
                "property_type" : "house"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):

        payload="H_Url=http%3A%2F%2Fcentroaffittifirenze.it%2Fit%2FAffitti%2F&Src_Li_Tip=A&Src_Li_Cat=&Src_Li_Cit=&Src_Li_Zon=&Src_T_Pr1=Prezzo+da&Src_T_Pr2=Prezzo+a&Src_T_Mq1=Mq+da&Src_T_Mq2=Mq+a&Src_T_Cod=Codice&Src_Li_Ord="

        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'http://centroaffittifirenze.it',
            'Referer': 'http://centroaffittifirenze.it/it/Affitti/',
            'Accept-Language': 'tr-TR,tr;q=0.9,en-GB;q=0.8,en;q=0.7,en-US;q=0.6',
        }

        url="http://centroaffittifirenze.it/ajax.html?azi=Archivio&lin=it&n="
    
        yield Request(
            url,
            body=payload,
            headers=headers,
            callback = self.parse_listing,
            dont_filter=True,
            meta={"property_type" : response.meta.get("property_type")}
            )

    def parse_listing(self,response):
        try:
            data = json.loads(response.body)["d"]
            sel = Selector(text=data, type="html")
            
            for item in sel.xpath("//div[contains(@class,'annuncio')]"):
                follow_url = response.urljoin(item.xpath(".//a//@href").get())
                prop_type = item.xpath(".//div[contains(@class,'categoria')]/text()").get()
                if get_p_type_string(prop_type):
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})
        except:
            pass

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Codice')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        description=response.xpath("//div[@class='desc']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        rent=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Prezzo')]//following-sibling::div//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("EUR")[0])
        item_loader.add_value("currency","EUR")

        utilities=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Spese condominiali annue')]//following-sibling::div//text()").get()
        if utilities:
            utilities = utilities.split("EUR")[0].split(",")[0].replace(".","").split(".")[0].strip()
            utilities = (int(utilities))
            item_loader.add_value("utilities", int(utilities/12))

        address="".join(response.xpath("//div[@class='mappaIndirizzo']/text()").getall())
        if address:
            address = address.split('via ')[-1].split('Via ')[-1]
            item_loader.add_value("address", address)

        item_loader.add_value("city", "Florence")

        square_meters=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Mq')]//following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        energy_label=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Classe')]//following-sibling::div//text()").get()
        if '(' in energy_label:
            item_loader.add_value("energy_label",energy_label.split("(")[0])
        else:
            item_loader.add_value("energy_label",energy_label)

        room_count=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Locali')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//div[@class='caratteristiche']//div[@class='sCar']//div[contains(.,'Bagni')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        parking=response.xpath("//div[@class='label'][contains(.,'Posti auto')]/following-sibling::div/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        images=[x for x in response.xpath("//div[@class='fotorama']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images) 
        latitude=response.xpath("//script[contains(.,'map_canvas')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("map_canvas")[-1].split(",")[1].strip())
            item_loader.add_value("longitude",latitude.split("map_canvas")[-1].split(",")[2].strip())

        item_loader.add_value("landlord_name", "Centro Affitti Firenze")
        item_loader.add_value("landlord_phone", "055/5520646")
        item_loader.add_value("landlord_email", "info@centroaffittifirenze.it")

        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower() or " local" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "stanza" in p_type_string.lower():
        return "room"
    else:
        return "house"