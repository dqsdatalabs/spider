# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = "cedimmo_be"
    start_urls = ['https://cedimmo.be/nos-biens/a-louer']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    custom_settings = {
        "FEED_EXPORT_ENCODING" : "utf-8"
    }
    
    def parse(self, response):
        token = response.xpath("//input[@name='_token']/@value").get()
        headers = {
            "origin": "https://cedimmo.be",
            "referer": "https://cedimmo.be/nos-biens/a-louer",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        yield FormRequest(
            url="https://cedimmo.be/property/search",
            formdata={
                "_token": token,
                "negotiation": "let",
                "search": "",
                "type": "house",
                "budgetMin": "",
                "budgetMax": "",
                "sold": "false",
                "start_index": "0",
                "count": "100",
                "sort[column]": "createdAt",
                "sort[direction]": "desc",
            },
            callback=self.after_post,
            headers=headers,
        )
        yield FormRequest(
            url="https://cedimmo.be/property/search",
            formdata={
                "_token": token,
                "negotiation": "let",
                "search": "",
                "type": "apartment",
                "budgetMin": "",
                "budgetMax": "",
                "sold": "false",
                "start_index": "0",
                "count": "100",
                "sort[column]": "createdAt",
                "sort[direction]": "desc",
            },
            callback=self.after_post,
            headers=headers,
        )
    def after_post(self, response):
        result = json.loads(response.text)
        for prop in result["result"]["result"]:
            if prop["estateStatus"] != "rented":
                item_loader = ListingLoader(response=response)
                item_loader.add_value(
                    "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
                )
                item_loader.add_value('title', prop["title"])
                item_loader.add_value('address', prop["address"])
                if prop["price"]:
                    item_loader.add_value('rent', int(prop["price"]))
                item_loader.add_value('currency', prop["priceCurrency"])
                item_loader.add_value('zipcode', prop["postalCode"])
                item_loader.add_value('city', prop["city"])
                if prop["bedrooms"]:
                    item_loader.add_value('room_count', prop["bedrooms"])
                if prop["bathrooms"]:
                    item_loader.add_value('bathroom_count', prop["bathrooms"])
                if prop["size"]:
                    item_loader.add_value('square_meters', re.sub(r"[^\d]", "", prop["size"]))
                item_loader.add_value('property_type', prop["estateType"])
                imgs = [
                    "".join(["https://cedimmo.be/attachment/render/", img["id"], "/700x700"])
                    for img in prop["images"]
                ]
                item_loader.add_value('images', imgs)
                if prop["data"]["location"].get("floor"):
                    item_loader.add_value('floor', prop["data"]["location"]["floor"])

                desc = prop["data"]["description"]["fr"]
                item_loader.add_value('description', desc)             

                item_loader.add_value(
                    'furnished',
                    prop["data"]["features"]["comfort"]["furnished"],
                )
                item_loader.add_value(
                    'landlord_name',
                    " ".join(
                        [
                            prop["data"]["negotiator"]["first_name"],
                            prop["data"]["negotiator"]["last_name"],
                        ]
                    ),
                )
                item_loader.add_value('landlord_phone', prop["data"]["negotiator"]["phone"])
                item_loader.add_value('landlord_email', prop["data"]["negotiator"]["email"])
                if prop["data"]["price_costs"]:
                    item_loader.add_value('utilities', (prop["data"]["price_costs"]["fr"]))
                item_loader.add_value(
                    'latitude',
                    str(prop["data"]["location"]["geo"]["latitude"]),
                )
                item_loader.add_value(
                    'longitude',
                    str(prop["data"]["location"]["geo"]["longitude"]),
                )

                est_type = prop["estateType"]
                postal_code = prop["postalCode"]
                city = prop["city"].replace(" ","")
                auto_id = str(prop["autoId"])
                ext_url = "https://cedimmo.be/nos-biens/a-louer/" + est_type + "/" + postal_code + "-" + city + "/" + auto_id
                item_loader.add_value('external_link', ext_url)



                for park in prop["data"]["amenities"]:
                    # print(x)
                    if "parking" == park:
                        item_loader.add_value('parking', True)
                    elif 'terrace' == park:
                        item_loader.add_value('terrace', True)
                    elif park == "lift":
                        item_loader.add_value('elevator', True)
                    elif 'balcony' == park:
                        item_loader.add_value('balcony', True)
                yield item_loader.load_item()

    def get_from_detail_panel(self, node, key):
        node.get()
        return node.xpath("".join([".//td[contains(.,'", key, "')]/following-sibling::td[1]"]))
