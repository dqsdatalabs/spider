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
    name = 'ladresse_com'
    execution_type='testing'   
    country='france'
    locale='fr'
    external_source = "Ladresse_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ladresse.com/type_bien/3-32/a-louer.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.ladresse.com/type_bien/4-39/a-louer.html",
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
        for item in response.xpath("//div[@class='products-cell']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        commercial = "".join(response.xpath("//div/h1/text()[contains(.,'commercial')]").extract())
        rented = "".join(response.xpath("//div/h1/text()[contains(.,'vendre')]").extract())
        non_residential = response.xpath("//h1[contains(., 'Parking') or contains(., 'parking')]/text()").get()
        if commercial or rented or non_residential:
            return


        item_loader.add_value("external_source", self.external_source)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title.replace("L'ADRESSE","").replace("L'Adresse","").replace("l'ADRESSE","").replace("l'Adresse","").replace("l'adresse","").replace("L'adresse",""))

        meters = "".join(response.xpath("//ul/li[2]/span[@class='critere-value']/text()[contains(.,'m²')]").extract())
        if meters:
            item_loader.add_value("square_meters", int(float(meters.split("m²")[0].replace(",",".").strip())))
        else:
            meters = "".join(response.xpath("//ul/li/span[@class='critere-value']/text()[contains(.,'m²')]").extract())
            if meters:
                item_loader.add_value("square_meters", int(float(meters.split("m²")[0].replace(",",".").strip())))

        
        room = "".join(response.xpath("//ul/li/span[@class='critere-value']/text()[contains(.,'Pièces')]").extract())
        if room:
            item_loader.add_value("room_count", room.split("Pièces")[0])
    
        bathroom_count=response.xpath("//ul/li/img[contains(@src,'bain')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        price = "".join(response.xpath("normalize-space(//div[@class='prix loyer']/span[@class='alur_loyer_price']/text())").extract())
        if price :
            rent = price.split(" ")[1].strip().split("€")[0].replace(",",".").replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")


        city_zipcode = response.xpath("//span[@class='alur_location_ville']/text()").extract_first()
        if city_zipcode:
            zipcode= city_zipcode.split(" ")[0].strip()
            city= city_zipcode.split(" ")[-1].strip()
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode.strip())
            # if  re.search("Marseille",zipcode):
            #     item_loader.add_value("zipcode", '13003')
            if zipcode.isalpha():
                text=response.xpath("//body/script/text()").get()
                text=text.replace("\t","").replace("\n","")
                index=text.find("item-ville")
                r=text[index:]
                rc=re.findall("\d+",r)
                zzipcode=rc[0]
                item_loader.add_value("zipcode", zzipcode)




            item_loader.add_value("city", city.strip())
            if city_zipcode:
                item_loader.add_value("address", city_zipcode.strip())
            else:
                address = response.xpath('//h1/text()').get()
                if address:
                    item_loader.add_value('address', address.split()[-1].strip())

        external_id = "".join(response.xpath("//span[@itemprop='name']/text()[contains(.,'Ref.')]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        floor=response.xpath("//li[img[contains(@src,'etage')]]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0])
        
        desc = "".join(response.xpath("//div[@class='content-desc']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.replace("L'ADRESSE","").replace("L'Adresse","").replace("l'ADRESSE","").replace("l'Adresse","").replace("l'adresse","").replace("L'adresse",""))

        label = "".join(response.xpath("//div[@class='product-dpe'][1]/div/@class").extract())
        if label :
            item_loader.add_value("energy_label", label.split(" ")[1].split("-")[1].upper())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='sliders-product']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        deposit=response.xpath("//span[contains(@class,'depot')]/text()").get()
        if deposit:
            dep = deposit.split(":")[1].split("€")[0].replace(" ","").replace("\xa0","")
            item_loader.add_value("deposit",int(float(dep)) )
        
        utilities=response.xpath("//span[contains(@class,'charges')]/text()").get()
        if utilities:
            try:
                item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].replace(" ",""))
            except:
                pass
        
        furnished=response.xpath("///ul/li/span[@class='critere-value']/text()[contains(.,'Meublée') or contains(.,'Aménagée') or contains(.,'équipée')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished=response.xpath("//span[@class='alur_location_meuble']//text()[contains(.,'meublé')]").extract_first()
            if furnished:
                item_loader.add_value("furnished", True)

        parking=response.xpath("//ul/li/span[@class='critere-value']//preceding::img/@src[contains(.,'garage')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@class='product-description']/h2//text()").extract_first()
            if parking:
                if "parking" in parking.lower():
                    item_loader.add_value("parking", True)
        try:
            latitude_longitude="".join(response.xpath("//script[contains(.,'lat')]/text()").extract())
            if latitude_longitude:
                lat=latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
                lng=latitude_longitude.split('LatLng(')[1].split(',')[1].split(");")[0]
                item_loader.add_value("latitude",lat.strip())
                item_loader.add_value("longitude", lng.strip())
        except:
            pass
        landlord_name = " ".join(response.xpath("//div[@class='product-agence']/div[@class='agence-title']//text()").getall()).strip()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.split('(')[0].strip())

        landlord_phone = response.xpath("//div[@class='product-agence']/div[@class='agence-telephone']//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(':')[-1].strip())
        status  = external_id
        if status:
            yield item_loader.load_item()