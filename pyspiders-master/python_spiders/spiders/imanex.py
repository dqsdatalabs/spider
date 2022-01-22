# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class ImanexSpider(scrapy.Spider):
    name = "imanex"
    allowed_domains = ["imanex.be"]
    start_urls = (
        "https://www.imanex.be/nl/te-huur/appartementen",
        "https://www.imanex.be/nl/te-huur/woningen",
        "https://www.imanex.be/nl/te-huur/studio-s",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):

        """parse list page and send requests to detail page, read fields if exist """
        for link in response.xpath(".//div[div[contains(@class,'property-info')]]"):
            # print(link.get())

            if link.xpath(".//*[contains(@class,'label sold')]").get():
                pass
            else:
                item = {}
                item["address"] = link.css(".prop-address").xpath("./text()").get()
                item["city"] = link.css(".prop-city").xpath("./text()").get().strip()
                item["property_type"] = "house" if "woningen" in response.url else "apartment"
                item["rent_string"] = link.css(".prop-price").xpath("./text()").get()
                item["room_count"] = link.xpath(".//div[i[contains(@class,'property-icon-bed')]]//text()").get()
                item["bathroom_count"] = link.xpath(".//div[i[contains(@class,'property-icon-shower')]]//text()").get()
                item["square_meters"] = link.xpath(".//div[i[contains(@class,'property-icon-surface')]]//text()")
                yield scrapy.Request(
                    response.urljoin(link.xpath(".//a/@href").get()),
                    self.parse_detail,
                    cb_kwargs=dict(item=item),
                )

        yield from self.parse_next(response)

    def parse_next(self, response):
        """ parse next page"""
        xpath = ".//a[@title='Volgende']"
        for link in response.xpath(xpath):
            yield response.follow(link, self.parse)

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"\d+\.\d{5,},\d+\.\d{5,}", response.text)
        if geo:
            geo = geo.group().split(",")
            item_loader.add_value("latitude", geo[0])
            item_loader.add_value("longitude", geo[1])

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        self.parse_map(response, item_loader)
        for k, v in item.items():
            item_loader.add_value(k, v)
        item_loader.add_xpath("title", ".//div[@class='row-fluid header']/div/h3[1]/text()")
        zipcode = response.xpath("//div[@class='row-fluid header']/div/h3[1]/text()").get()
        if zipcode and "(" in zipcode:
            zipcode = zipcode.split("(")[-1].split(")")[0]
            item_loader.add_value("zipcode", zipcode)
        item_loader.add_xpath("description", "//div[@class='group description']//text()")
        item_loader.add_xpath("images", ".//div[@id='LargePhoto']//a[contains(@class,'colorBoxImg')]/@href")
        item_loader.add_xpath(
            "utilities", ".//div[div[@class='name'][contains(.,'Totale lasten huurder')]]/div[@class='value']/text()"
        )
        dt = response.xpath(".//div[div[@class='name'][contains(.,'Vrij')]]/div[@class='value']/text()").get()
        if dt:
            dt = dateparser.parse(dt)
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )

        self.get_from_detail_panel(
            " ".join(
                response.xpath(".//div[div[@class='value'][not(contains(.,'Nee'))]]/div[@class='name']/text()").getall()
            ),
            item_loader,
        )

        item_loader.add_xpath("landlord_email", ".//a[contains(@href,'mailto:')]//text()")
        item_loader.add_value("landlord_phone", "011/672 201")
        item_loader.add_value("landlord_name", "IMANEX NV")
        # item_loader.add_xpath("parking","./div[@class='name']/text()[contains(.,'Garage')]")
        yield item_loader.load_item()

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balcon",
                "nombre de balcon",
                "Nombre d",
                "balcony",
                "balcon arrière",
            ],
            "pets_allowed": ["animaux"],
            "furnished": ["meublé", "appartement meublé", "meublée"],
            "swimming_pool": ["piscine"],
            "dishwasher": ["lave-vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["lift", "ascenceur"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)