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
    name = 'lille_immodefrance_nord_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_urls = "https://lille.immodefrance-nord.com/fr/locations"  # LEVEL 1
    headers = {
        ':authority': 'lille.immodefrance-nord.com',
        ':method': 'POST',
        ':path': '/fr/locations',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'origin': 'https://lille.immodefrance-nord.com',
        'referer': 'https://lille.immodefrance-nord.com/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
    }
    custom_settings = {
        "PROXY_ON" : "True"
    }

    def start_requests(self):
        formdata = {
            "location_search[commune]": "",
            "location_search[typeBien][]": "1",
            "location_search[loyer_min]": "39",
            "location_search[loyer_max]": "1000000",
            "location_search[surface_min]": "",
            "location_search[noMandat]": "",
            "location_search[tri]": "loyerCcTtcMensuel|asc",
        }
        yield FormRequest(url=self.post_urls,
                    callback=self.parse,
                    formdata=formdata,
                    headers=self.headers,
                    meta={"property_type": "apartment",})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 20)
        seen = False
        for item in response.xpath("//div[contains(@class,'article')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                    follow_url,
                    callback=self.populate_item,
                    meta={
                        "property_type": response.meta.get('property_type')
                    }
                )
            seen = True
        
        if page == 20 or seen:
            url = f"https://lille.immodefrance-nord.com/fr/map/mini-fiche/Location/{page}/normal/loyerCcTtcMensuel%7Casc"
            yield Request(url,
                    callback=self.parse,
                    headers=self.headers,
                    meta={
                        "page": page+20,
                        "property_type": response.meta.get('property_type')
                    }
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        external_id = response.xpath("//div[contains(@class,'titre')]//h3//text()").get()
        if external_id:
            external_id = external_id.split("Réf. :")[1].strip().split(" ")[0]
            item_loader.add_value("external_id", external_id)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Lille_Immodefrance_Nord_PySpider_france", input_type="VALUE")

        address = response.xpath("//div[contains(@class,'titre')]//h3//text()").get()
        if address:
            address1= address.split(")")[0].strip().replace("(","")
            city = address.split(")")[0].strip().split("(")[0]
            zipcode = address.split(")")[0].strip().split("(")[1]
            item_loader.add_value("address",address1)
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)

        room_count = (response.url).split("appartement-")[1].split("-")[0]
        if room_count:
            item_loader.add_value("room_count",room_count)
            
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'descriptif')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'criteres')]//div[contains(.,'Surface habitable')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"m":0,".":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[contains(@class,'chambre')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(@class,'sdb')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(@class,'prix')]//span[contains(@class,'prix')]/text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[contains(@class,'criteres')]//div[contains(.,'garantie')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'carousel-photo')]//@src", input_type="M_XPATH")
        energy_label = "".join(response.xpath("//span[contains(.,'Consommation énergétique')]//parent::div//td[contains(.,'*')]//text()").getall())
        if energy_label:
            energy_label = energy_label.split("*")[0].strip()
            item_loader.add_value("energy_label", energy_label)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'position =')]/text()", input_type="F_XPATH", split_list={"position = [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'position =')]/text()", input_type="F_XPATH", split_list={"position = [":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[contains(@class,'criteres')]//div[contains(.,'Étage')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[contains(@class,'criteres')]//div[contains(.,'parking') or contains(.,'garage')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[contains(@class,'criteres')]//div[contains(.,'Terrasse')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LILLE IMMO DE FRANCE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 28 36 88 36", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="vente.arts@immodefrance.com", input_type="VALUE")
        
        yield item_loader.load_item()