# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from scrapy.http import FormRequest
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class DicasaSpider(scrapy.Spider):
    name = "dicasa"
    allowed_domains = ["dicasa.be"]
    start_urls = ["https://dicasa.be/te-huur"]
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","
    lists = ["16", "26"]
    index = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        """parse list page and send requests to detail page, read fields if exist """
        token = response.xpath(".//input[@id='formTokenFilterForm']/@value").get()
        yield FormRequest(
            url="https://dicasa.be/te-huur",
            formdata={
                "form": "filterForm",
                "form_token": token,
                "text": "",
                "regions": "",
                "types": "12",
                "priceFrom": "",
                "priceTo": "",
            },
            callback=self.after_post,
        )

    def after_post(self, response):
        """ after post ,read link"""

        for link in response.xpath(".//div[div[contains(@class,'contentdiv') and contains(.,'€')]]"):
            item = {
                "room_count": link.xpath("./div[@class='contentdiv']/div/div[1]//text()").get(),
                "bathroom_count": link.xpath("./div[@class='contentdiv']/div/div[2]//text()").get(),
                "rent_string": link.xpath("./div[@class='contentdiv']/div/div[3]/span/text()").get(),
                "property_type": "house" if "maison" in response.url else "apartment",
            }
            if item.get("rent_string"):
                yield scrapy.Request(
                    response.urljoin(link.xpath("./a/@href").get()),
                    self.parse_detail,
                    cb_kwargs=dict(item=item),
                )
        yield from self.parse_next(response)

    def parse_next(self, response):
        """ parse next page"""
        xpath = ".//a[i[@class='fas fa-chevron-right'] and @class='sidebtn' and @href ]"
        if response.xpath(xpath).get():
            for link in response.xpath(xpath):
                yield response.follow(link, self.after_post, dont_filter=True)
        else:
            token = response.xpath(".//input[@id='formTokenFilterForm']/@value").get()
            self.index += 1
            if len(self.lists) > self.index - 1:
                yield FormRequest(
                    url="https://dicasa.be/te-huur",
                    formdata={
                        "form": "filterForm",
                        "form_token": token,
                        "text": "",
                        "regions": "",
                        "types": self.lists[self.index - 1],
                        "priceFrom": "",
                        "priceTo": "",
                    },
                    callback=self.after_post,
                )

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"lat: \d+\.\d{3,}", response.text)
        geo1 = re.search(r"lng: \d+\.\d{3,}", response.text)
        if geo and geo1:
            geo = geo.group().split(":")
            geo1 = geo1.group().split(":")
            item_loader.add_value("latitude", geo[-1].strip())
            item_loader.add_value("longitude", geo1[-1].strip())
            # self.get_from_geo(item)

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)
        for k, v in item.items():
            item_loader.add_value(k, v)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        main_xpath = ".//*[contains(@class,'detailcontent')]"
        
        item_loader.add_xpath("title", f"{main_xpath}/div[1]/h1/text()")
        item_loader.add_xpath("address", f"{main_xpath}/div[1]/h2/text()")
        item_loader.add_xpath("description", f"{main_xpath}/div[3]//text()")
        item_loader.add_xpath("images", ".//div[@class='carousell']//a/@href")
        ext_id = response.url.split("id=")[1].split("#")[0]
        if ext_id:
            item_loader.add_value("external_id", ext_id.strip())
        city = response.xpath("substring-after(//*[contains(@class,'detailcontent')]/div[1]/h2/text(),',')").extract_first()
        if city:
            zipcode = city.strip().split(" ")[0].strip()
            city = " ".join(city.strip().split(" ")[1:])
            if "(" in city:
                city = city.split("(")[0]
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode)
        item_loader.add_xpath("landlord_email", ".//*[@class='quickinfo']//li[@class='mail']/a/@href")
        item_loader.add_xpath("landlord_phone", ".//*[@class='quickinfo']//li[@class='tel']//text()")
        item_loader.add_value("landlord_name", "Immo DICASA")
        dt = response.xpath(".//tr[contains(.,'Komt vrij op')]/td[@class='kolom2']/text()").get()
        if dt:
            dt = dateparser.parse(dt)
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )
        self.get_general(response, item_loader)
        self.get_from_detail_panel(
            " ".join(response.xpath(".//div[@class='details']//tr//text()").getall()), item_loader
        )
        self.parse_map(response, item_loader)
        yield item_loader.load_item()

    def get_general(self, response, item_loader):
        keywords = {
            "square_meters": "Bewoonbare oppervlakte",
            "floor": "etage",
            "utilities": "Gemeenschappelijke kosten",
            # "room_count": "Slaapkamers",
            # "bathroom_count": "Salle de bains",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//*[@class='details']//tr[td[contains(.,'{v}')]]/td[@class='kolom2']//text()")

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
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

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)
