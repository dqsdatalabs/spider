# -*- coding: utf-8 -*-

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import math

class SatospiderSpider(Spider):
    name = "sato_fi"
    external_source = "Sato_fi_PySpider_finland"
    execution_type='testing'
    country='finland'
    locale='fi' 
    start_urls = ['https://oma.sato.fi/api/realestates/v1/search'] 

    payload = {"page":{"fromIndex":0,"pageSize":25},
                          "sort":{"field":"VACANCY"},
                          "rules":[]
                          }

    headers = {
        "authority": "oma.sato.fi",
        "method": "POST",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    def start_requests(self):
        
        yield Request(
            url=self.start_urls[0],
            headers=self.headers,
            method="POST",
            body=json.dumps(self.payload),
            callback=self.parse
        )
    def parse(self, response, **kwargs):

        data = json.loads(response.body)

        total_num = data["totalResultCount"]
        data = data["apartments"]

        for item in data:
            item_loader = ListingLoader(response=response)

            room_count = item["apartment"]["amountOfRooms"]
            if room_count:
                item_loader.add_value("room_count",room_count)

            square_meters = item["apartment"]["livingArea"]
            if square_meters:
                item_loader.add_value("square_meters",int(square_meters))

            floor = item["apartment"]["floorNumber"]
            if floor:
                item_loader.add_value("floor",floor)

            energy_label = item["apartment"]["energyClass"]
            if energy_label:
                if len(energy_label) == 1:
                    item_loader.add_value("energy_label",energy_label)


            rent = item["apartment"]["rent"]
            if rent:
                item_loader.add_value("rent",int(rent))

            features = item["apartment"]["features"]
            if features:
                if "BALCONY" in features:
                    item_loader.add_value("balcony",True)
                if "PETS_ALLOWED" in features:
                    item_loader.add_value("pets_allowed",True)
                if "ELEVATOR" in features:
                    item_loader.add_value("elevator",True)

            medias = item["medias"]
            if medias:
                images = []
                for image in medias:
                    base_url = "https://res.cloudinary.com/sato/image/upload/c_fill,dpr_auto,f_auto,q_auto,w_1600/v1/"
                    img = base_url + image["cloudinaryId"]
                    images.append(img)

                item_loader.add_value("images",images)
                item_loader.add_value("external_images_count",len(images))

            title = item["apartment"]["name"]
            if title:
                item_loader.add_value("title",title)

            city = item["areas"]["municipality"]["name"]
            if city:
                item_loader.add_value("city",city)

            street = item["apartment"]["streetAddress"]
            if street:
                item_loader.add_value("address",street + " " + city)
 
            next_url = "https://www.sato.fi/en/rental-apartments/" + \
                        item["areas"]["municipality"]["name"] +"/"+ \
                        item["areas"]["area"]["name"] + "/"+\
                        item["apartment"]["name"] +"/"+ str(item["apartment"]["realEstateId"]) + \
                        "/apartment/" + str(item["id"])

            next_url = next_url.replace(" ","%20")
            item_loader.add_value("external_link",next_url)
            item_loader.add_value("external_id",next_url.split("apartment/")[-1])
            
                        

            yield Request(next_url,callback=self.populate_item,meta={"item_loader":item_loader})


        page = response.meta.get("page",25)
        if page <= total_num:
            self.payload["page"]["fromIndex"]=page

            yield Request(
            url=self.start_urls[0],
            headers=self.headers,
            method="POST",
            body=json.dumps(self.payload),
            callback=self.parse,
            meta={"page":page+25}
        )

    def populate_item(self, response):
        item_loader = response.meta.get("item_loader")
        desc = response.xpath("//meta[@name='description']/@content").get()
        if desc:
            item_loader.add_value("description",desc)

        item_loader.add_value("currency","EUR")

        position = response.xpath("//a[contains(@title,'Open')]/@href").get()
        if position:
            lat = re.search(r"ll=([\d.]+),",position).group(1)
            long = re.search(",([\d.]+)&",position).group(1)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        item_loader.add_value("landlord_email","asiakaspalvelu@sato.fi")
        item_loader.add_value("landlord_phone","020 334 443")
        item_loader.add_value("landlord_name","SATO RENTAL APARTMENTS")
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type","apartment")

        utilities = response.xpath("//h6[text()='Water rate']/parent::div/following-sibling::div/p/text()").get()
        if utilities:
            utilities = re.search("[\d]+",utilities)
            if utilities:
                utilities=utilities[0]
            item_loader.add_value("utilities",utilities)

        zip = response.xpath("//div[h6[text()='Location']]/following-sibling::div/p/a/text()").get()
        if zip:
            zipcode = re.search("[, ]([\d]{3,})[, ]",zip).group(1)
            item_loader.add_value("zipcode",zipcode)

        yield item_loader.load_item()