# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import json
import re
import scrapy
from scrapy.http import FormRequest
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *


class ImmocovaSpider(scrapy.Spider):
    name = "immocova"
    allowed_domains = ["immocova.be"]
    start_urls = ("http://www.immocova.be/",)
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.get_lang(),
            )

    def parse(self, response, **kwargs):
        form_datas = [
            {"page": "tehuur", "search": "", "type": "Appartement", "price": "", "number": ""},
            {"page": "tehuur", "search": "", "type": "Woning", "price": "", "number": ""},
        ]
        for form_data in form_datas:
            yield FormRequest(
                url="http://www.immocova.be/controller.php?controller=products",
                formdata=form_data,
                dont_filter=True,
                callback=self.after_post,
                headers=self.get_lang(),
                method="POST",
                cb_kwargs=dict(form=form_data),
            )

    def after_post(self, response, form):
        json_data = json.loads(response.text)
        json_data = json.loads(json_data["products"])
        for x in json_data:
            x["type"] = form["type"]
            yield Request(
                f"http://www.immocova.be/details.php?CODE={x['code']['0']}",
                self.parse_detail,
                headers=self.get_lang(),
                cb_kwargs=dict(x=x),
            )

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"\d+\.\d{5,},\d+\.\d{5,}", response.text)
        if geo:
            geo = geo.group().split(",")
            item_loader.add_value("latitude", geo[0])
            item_loader.add_value("longitude", geo[1])
        yield item_loader.load_item()

    def parse_detail(self, response, x):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("CODE=")[1])
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        # item_loader.add_value("title", x["typeTitle"])
        item_loader.add_value("rent", x["price"]["0"])
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("room_count", x["bed"]["0"])
        item_loader.add_value("city", x["location"]["0"])
        item_loader.add_value("property_type", "house" if x["type"] == "Woning" else "apartment")
        if x["bath"]:
            item_loader.add_value("bathroom_count", x["bath"]["0"])

        square = response.xpath("//div[./h6[contains(.,'Bewoonbare oppervlakte')]]/following-sibling::div/h6/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        main_block_xpath = ".//*[@class='product-info']"
        main_block = response.xpath(main_block_xpath)
        if len(main_block) == 1:
            item_loader.add_xpath("title", f"{main_block_xpath}//h3[@class='title']/text()")
            item_loader.add_xpath("address", f"{main_block_xpath}//*[@class='location']/text()")

            item_loader.add_xpath("description", f"{main_block_xpath}//ul[@class='description']//text()")

            # item_loader.add_xpath("available_date", ".//li[contains(.,'Vrij')]/text()")

            item_loader.add_xpath("room_count", f"{main_block_xpath}//span[@class='room-item'][1]/text()")

            item_loader.add_xpath("images", ".//div[@id='product-carousel']//img/@src")

            for k, v in self.get_from_detail_panel(main_block):
                if type(v) == str:
                    item_loader.add_xpath(k, v)
                else:
                    item_loader.add_value(k, v)
            contact_node = response.css(".infocontact")
            item_loader.add_value("landlord_name", contact_node.xpath(".//h5/text()").get())
            item_loader.add_value("landlord_phone", contact_node.xpath(".//p[contains(.,'Tielt')]/text()").get())
            item_loader.add_value(
                "landlord_email", contact_node.xpath(".//a[contains(@class,'contactToMail')]/@href").get()
            )
            yield Request(
                response.xpath(".//iframe[@class='maps']/@src").get(),
                self.parse_map,
                dont_filter=True,
                cb_kwargs=dict(item_loader=item_loader),
            )

    def get_from_detail_panel(self, node):
        keywords = {
            "parking": [
                "parking",
                "garage",
                "parkeerplaatsen",
                "garagepoort",
                "parkeerplaats",
                "ondergrondse staanplaats",
            ],
            "balcony": ["balkon"],
            "pets_allowed": ["huisdieren toegelaten"],
            "furnished": ["gemeubileerd", "bemeubeld", "ingericht", "ingerichte", "gemeubeld"],
            "swimming_pool": ["zwembad"],
            "dishwasher": ["vaatwasser", "vaatwas", "afwasmachine"],
            "washing_machine": ["wasmachine"],
            "terrace": ["terras", "oriëntatie tuin/terras"],
            "elevator": ["lift", "elevator"],
        }
        for sub_node in node.xpath(".//div[count(div/h6)=2]"):
            title = sub_node.xpath("./div[1]/h6/text()").get()
            value = remove_white_spaces(title).casefold()
            for k, v in keywords.items():
                if any(s in value for s in v) and "Ja" == sub_node.xpath("./div[2]/h6/text()").get():
                    yield k, True

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }