# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmodhondtSpider(scrapy.Spider):
    name = "immodhondt"
    allowed_domains = ["immodhondt.be"]
    start_urls = (
        "https://www.immodhondt.be/nl/te-huur?view=list&page=1&ptype=1",
        "https://www.immodhondt.be/nl/te-huur?view=list&page=1&ptype=2",
        "https://www.immodhondt.be/nl/te-huur?view=list&page=1&ptype=3",
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
        for item_responses in response.css(".property-list").xpath("div[@class='row-fluid']//a"):
            link = item_responses.xpath(".//@href").get()
            if link:
                yield scrapy.Request(
                    response.urljoin(link),
                    self.parse_detail,
                    cb_kwargs=dict(property_type="house" if "&ptype=1" in response.url else "apartment"),
                )
        yield from self.parse_next(response)

    def parse_next(self, response):
        """parse next page """
        xpath = './/a[@class="nav next"]/@href'
        for link in response.xpath(xpath).getall():
            yield response.follow(response.urljoin(link), self.parse)

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"\d+\.\d{5,},\s*\d+\.\d{5,}", response.text)
        if geo:
            geo = geo.group().split(",")
            item_loader.add_value("latitude", geo[0])
            item_loader.add_value("longitude", geo[1])
            # self.get_from_geo(item)

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_xpath("title", ".//head/meta[@property='og:title']/@content")
        item_loader.add_xpath("description", ".//head/meta[@property='og:description']/@content")
        item_loader.add_xpath("images", ".//div[@class='swiper-slide']/img/@src")
        item_loader.add_xpath("rent", ".//div[@class='detailproperty-price']//text()")

        item_loader.add_xpath("property_type", ".//input[@name='FIELD[Property_Type]']/@value")
        item_loader.add_xpath("zipcode", ".//input[@name='FIELD[Property_Zip]']/@value")
        item_loader.add_xpath("city", ".//input[@name='FIELD[Property_City]']/@value")
        item_loader.add_xpath("external_id", ".//input[@name='FIELD[ObjectID]']/@value")
        item_loader.add_xpath(
            "square_meters", ".//div[@class='detailproperty-maininfo']//li[i[@class='icon icon-home']]//text()"
        )
        item_loader.add_xpath(
            "room_count", ".//div[@class='detailproperty-maininfo']//li[i[@class='fa fa-bed']]//text()"
        )
        item_loader.add_xpath(
            "bathroom_count", ".//div[@class='detailproperty-maininfo']//li[i[@class='fa fa-bathtub']]//text()"
        )
        if response.xpath(".//div[@class='detailproperty-maininfo']//li[i[@class='fa fa-car']]//text()").get():
            item_loader.add_value("parking", True)
        item_loader.add_xpath("landlord_phone", ".//div[@class='detman-info']//li[2]//text()")
        item_loader.add_xpath("landlord_name", ".//div[@class='detman-info']//li[1]//text()")
        item_loader.add_xpath("landlord_email", ".//div[@class='detman-info']//li[3]/a/text()")
        self.get_from_detail_panel(
            " ".join(response.xpath(f'.//div[div[@class="dp-value"][not(contains(.,"Nee"))]]/div[1]//text()').getall()),
            item_loader,
        )
        item_loader.add_xpath(
            "utilities", f'.//div[div[@class="dp-name"][contains(.,"Lasten huurder")]]/div[2]//text()'
        )
        item_loader.add_xpath("address", f'.//div[div[@class="dp-name"][.="Adres"]]/div[2]//text()')

        self.parse_map(response, item_loader)
        yield item_loader.load_item()

    def get_from_detail_panel(self, text, item_loader, bool_value=True):
        if not hasattr(self, "key_set"):
            self.key_set = set()
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balkon",
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
            "elevator": ["lift", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                if bool_value == False and k not in self.key_set:
                    item_loader.add_value(k, bool_value)
                else:
                    item_loader.add_value(k, bool_value)
                self.key_set.add(k)