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
    name = 'bourgogne_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
      "PROXY_ON": True,
    }
  
    def start_requests(self):
        url = "https://www.bourgogne-immobilier.com/fr/locations"
        headers = {
            'Origin': 'https://www.bourgogne-immobilier.com',
            'accept': 'text/html, */*; q=0.01',
            "accept-encoding" : "gzip, deflate, br",
            "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
            'Referer': 'https://www.bourgogne-immobilier.com/fr/locations',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
        }
        start_urls = [
            {
                "formdata" : {
                    'location_search[typeBien][]': '1',
                    "location_search[tri]": "loyerCcTtcMensuel|asc",
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'location_search[typeBien][]': '2',
                    "location_search[tri]": "loyerCcTtcMensuel|asc",
                    },
                "property_type" : "house",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], headers=headers, dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//div[contains(@class,'article')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", "Bourgogne_Immobilier_PySpider_france")

        external_id = response.xpath("//div[contains(@class,'ref')]//span[contains(@class,'reference')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'left prix')]/text() | //div[contains(@class,'left prix')]//span[contains(@class,'commune') or contains(@class,'cp')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//div[contains(@class,'left prix')]//span[contains(@class,'commune') or contains(@class,'cp')]//text()").getall())
        if address:
            item_loader.add_value("address", address)
        city = response.xpath("//div[contains(@class,'detail row')]//span[contains(@class,'commune')]//text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("//div[contains(@class,'detail row')]//span[contains(@class,'cp')]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.replace("(","").replace(")",""))
        
        desc = " ".join(response.xpath("//div[contains(@id,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        rent = "".join(response.xpath("//div[contains(@class,'charges')]//span[contains(@class,'prix')]//text()").getall())
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//div[contains(@class,'charges')]//div[contains(.,'garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = " ".join(response.xpath("//div[contains(@class,'charges')][contains(.,'Dont')]//text()").getall())
        if utilities:
            utilities = re.sub('\s{2,}', ' ', utilities.strip())
            utilities = utilities.split("-")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        square_meters = response.xpath("//b[contains(@class,'labelIcone')][contains(.,'Surface')]/following-sibling::b//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//b[contains(@class,'labelIcone')][contains(.,'Chambre')]/following-sibling::b//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//b[contains(@class,'labelIcone')][contains(.,'pièces')]/following-sibling::b//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//b[contains(@class,'labelIcone')][contains(.,'Salle de bains')]/following-sibling::b//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        floor = response.xpath("//b[contains(@class,'labelIcone')][contains(.,'Étage')]/following-sibling::b//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        parking = response.xpath("//b[contains(@class,'labelIcone')][contains(.,'Parking')]/following-sibling::b//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        energy_label = "".join(response.xpath("//div[contains(@class,'valeur_conso')][contains(.,'*')]//text()").getall())
        if energy_label:
            energy_label = energy_label.replace("*","").strip()
            item_loader.add_value("energy_label", str(int(float(energy_label))))

        images = [x for x in response.xpath("//div[contains(@id,'carousel-photo')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'position = [')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('position = [')[1].split(',')[0]
            longitude = latitude_longitude.split('position = [')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "BOURGOGNE IMMOBILIER")
        item_loader.add_value("landlord_email", "contact-chalon@bourgogne-immobilier.com")
        item_loader.add_value("landlord_phone", "03 80 30 70 70")
        yield item_loader.load_item()