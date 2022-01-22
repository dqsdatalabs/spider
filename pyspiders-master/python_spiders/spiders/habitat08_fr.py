# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re
import dateparser


class MySpider(Spider):
    name = 'habitat08_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.'

    custom_settings = {
        "PROXY_ON" : True
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.habitat08.fr/Biens-a-louer/(type)/housing_accommodation/(offset)/0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.habitat08.fr/Biens-a-louer/(type)/housing_house/(offset)/0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 21)
        seen = False

        for item in response.xpath("//article[contains(@class,'for_rent_housing')]/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 21 or seen:
            follow_url = response.url.replace("(offset)/" + str(page - 21), "(offset)/" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 21})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Habitat08_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")

        address = " ".join(response.xpath("//div[@class='autogrid2']/address//text()").getall())  
        if address:
            address = re.sub("\s{2,}", " ", address)
            item_loader.add_value("address", address)

            zipcode = zip_ext(address)
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

        utilities = " ".join(response.xpath("substring-before(//div[@class='good-price']/span[@class='good-price-charges']/text(),',')").getall())  
        if utilities:
            uti = utilities.replace("dont","")
            item_loader.add_value("utilities", uti.strip())

        rent = " ".join(response.xpath("//div[@class='good-price']/span[@class='good-price-total']/text()").getall())  
        if rent:
            price = rent.replace(",",".").replace("€","").strip()
            item_loader.add_value("rent", int(float(price)))
            item_loader.add_value("currency", "EUR")

        room_count = " ".join(response.xpath("//li[span[contains(.,'Typologie')]]/span[3]/text()").getall())  
        if room_count:
            item_loader.add_value("room_count", room_count.replace("T","").strip())

        square_meters = " ".join(response.xpath("//li[span[contains(.,'Surface')]]/span[3]/text()").getall())  
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace("m","").strip())

        floor = " ".join(response.xpath("//li[span[contains(.,'Étage')]]/span[3]/text()").getall())  
        if floor:
            item_loader.add_value("floor", floor.replace("étage","").strip())

        available_date=response.xpath("//div[@class='mentions-list mtm']/span/text()").get()

        if available_date:
            date2 =  available_date.split("Disponible au")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)


        description = " ".join(response.xpath("//div[@class='clearfix wysiwyg mtm']/p/text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        images = [x.split("url(")[1].split(")")[0].strip() for x in response.xpath("//div[@class='item']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        label = " ".join(response.xpath("substring-before(//div[@class='box']/img/@src[contains(.,'dpe')],'_dpe.png')").getall())  
        if label:
            item_loader.add_value("energy_label", label.split("-")[-1].strip())

        elevator = "".join(response.xpath("//ul[@class='autogrid2']/li[contains(.,'Ascenseur')]/text()").getall())
        if elevator:
            item_loader.add_value("elevator",True)

        balcony = "".join(response.xpath("//ul[@class='autogrid2']/li[contains(.,'Balcon')]/text()").getall())
        if balcony:
            item_loader.add_value("balcony",True)

        parking = "".join(response.xpath("//ul[@class='autogrid2']/li[contains(.,'Garage')]/text()").getall())
        if parking:
            item_loader.add_value("parking",True)
        else:
            parking = "".join(response.xpath("//ul[@class='autogrid2']/li[contains(.,'stationnement')]/text()").getall())
            if parking:
                item_loader.add_value("parking",True)

        latitude_longitude = response.xpath("//img[contains(@src,'googleapis')]//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("markers=")[1].split(",")[0]
            longitude = latitude_longitude.split("markers=")[1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_phone", "03 24 58 37 37")
        item_loader.add_value("landlord_name", "HABITAT 08")
        item_loader.add_value("landlord_email", "dfouzari@habitat08.fr")

        yield item_loader.load_item()

        try:
            f_url = response.urljoin(response.xpath("//div[@class='box']/a/@href").get())
            jrep_id = f_url.split("(containers)/")[1].split("/")[0]
            yield Request(
                url=f"https://www.habitat08.fr/gmap/xhrMainCarto/(arr_parent_id)/{jrep_id}",
                callback=self.get_lat_lng,
                meta={
                    "item":item_loader,
                    "id":f_url.split("/")[-1],
                }
            )
        except : pass      

    
    # def get_lat_lng(self, response):
    #     item_loader = response.meta["item"]

    #     ext_id = response.meta["id"]
    #     data = json.loads(response.body)
    #     for item in data["arrPoints"]:
    #         if item["id"] == ext_id:
    #             item_loader.add_value("latitude", item["lat"])
    #             item_loader.add_value("longitude", item["lng"])
    #             break

    #     yield item_loader.load_item()

def zip_ext(addr):
    zipcode = ""
    if addr:
        for item in addr.strip().split(" "):
            if item.strip().isnumeric() and len(item.strip()) > 4:
                zipcode = item
                break
    
    return zipcode
