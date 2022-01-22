# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class SofimoBeSpider(scrapy.Spider):
    name = "sofimo"
    allowed_domains = ["sofimo.be"]
    start_urls = (
        "https://www.sofimo.be/nl/te-huur?view=list&page=1&goal=1&gpsradius=-1&ptype=1",
        "https://www.sofimo.be/nl/te-huur?view=list&page=1&goal=1&gpsradius=-1&ptype=2",
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
                    response.urljoin(link.split("&page")[0]),
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
        item_loader.add_xpath("title", ".//div[@id='PropertyRegion']//h3[@class='pull-left leftside']//text()")
        item_loader.add_xpath("description", ".//head/meta[@property='og:description']/@content")
        item_loader.add_xpath("images", ".//div[@class='galleria']//a/img/@src")
        item_loader.add_xpath("external_id", "//div[.='Unieke code']/following-sibling::div[@class='value']/text()")

        rent = response.xpath("//div[.='Prijs']/following-sibling::div[@class='value']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("€", "").replace(" ", ""))

        sqr = response.xpath("//div[.='Woongedeelte']/following-sibling::div[@class='value']/text()").get()
        if sqr:
            item_loader.add_value("square_meters", sqr.replace("m²", "").replace("m", "").strip())

        address = response.xpath("//div[.='Adres']/following-sibling::div[@class='value']/text()").get()
        if address:
            address = address.strip().split(",")[-1].strip().split(" ")
            zipcode = ""
            city = ""
            for i in address:
                if i.strip().isdigit():
                    zipcode = i.strip()
                else:
                    city = city + i.strip() + " "
            if city and zipcode:
                item_loader.add_value("zipcode", zipcode) 
                item_loader.add_value("city", city)
            

        item_loader.add_xpath(
            "room_count", f'.//div[div[@class="name"][.="Aantal slaapkamers"]]/div[@class="value"]//text()'
        )
        item_loader.add_xpath(
            "bathroom_count", f'.//div[div[@class="name"][.="Aantal badkamers"]]/div[@class="value"]//text()'
        )

        if "IEPER" in response.xpath(f'.//div[div[@class="name"][.="Adres"]]/div[3]//text()').get():
            item_loader.add_value("landlord_email", "sofimo.ieper@sofimo.be")
            item_loader.add_value("landlord_name", "Sofimo Ieper")
            item_loader.add_value("landlord_phone", "(057)202020")
        else:
            item_loader.add_value("landlord_email", "sofimo.roeselare@sofimo.be")
            item_loader.add_value("landlord_name", "Sofimo Roeselare")
            item_loader.add_value("landlord_phone", "(051)202020")

        self.get_from_detail_panel(
            " ".join(response.xpath(f'.//div[div[@class="value"][not(contains(.,"Nee"))]]/div[1]//text()').getall()),
            item_loader,
        )
        item_loader.add_xpath(
            "utilities", f'.//div[contains(@class,"detail-prop-descr")]//span[contains(.,"Syndic:")]//text()'
        )
        item_loader.add_xpath("address", f'.//div[div[@class="name"][.="Adres"]]/div[3]//text()')
        available_date = response.xpath("//div[div[@class='name'][contains(.,'Beschikbaarheid')]]/div[@class='value']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
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