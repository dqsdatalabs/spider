# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser
import re

class SothebysrealtySpider(scrapy.Spider):
    name = "sothebysrealty"
    allowed_domains = ["sothebysrealty.be"]
    execution_type = "testing"
    country = "belgium"
    locale = "en"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sothebysrealty.be/estates/?estate_status=for-rent_en&estate_subtypes=flat_en%2Cground-floor_en%2Cpenthouse_en%2Cduplex_en&nb_rooms_range=0&bathrooms_range=0&price_range=1000%2C15000",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.sothebysrealty.be/estates/?estate_status=for-rent_en&estate_subtypes=house_en%2Cmaison-de-maitre_en%2Cvilla_en&nb_rooms_range=0&bathrooms_range=0&price_range=1000%2C15000",
                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield scrapy.Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for link in response.xpath("//section[contains(@class,'main-list')]/div/a[contains(@class,'link-queries')]/@href").getall():
            yield scrapy.Request(
                response.urljoin(link),
                self.parse_detail,
                meta={"property_type": response.meta.get('property_type')},
            )
            
        pagination = response.xpath(".//div[@class='pagination']/a[.='Next »']/@href").get()
        if pagination:
            yield scrapy.Request(response.urljoin(link), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"lat: \d+[.]\d+, lng: \d+.\d+", response.text)
        if geo:
            geo = geo.group().split(",")
            item_loader.add_value("latitude", geo[0].split(":")[-1].strip())
            item_loader.add_value("longitude", geo[1].split(":")[-1].strip())

    def parse_detail(self, response):
     
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        self.parse_map(response, item_loader)
        
        item_loader.add_xpath("description", "//div[contains(@class,'right text')]//p//text()")
        item_loader.add_xpath("rent_string", ".//span[@class='devises eur']/text()")
        item_loader.add_xpath("energy_label", "//div[h3[.='PEB']]/p/text()")
  
        
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'swiper-wrapper')]/div[@class='swiper-slide']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        available_date= "".join(response.xpath("//div[h3[.='Availability']]/p/text()[.!='immediately']").getall())

        if available_date:
            date2 =  available_date.strip().replace("Immediate","Now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        self.get_general(item_loader)
        furnished = response.xpath(".//tr[td[contains(.,'Type of rental')]]/td[2]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        parking =" ".join(response.xpath(".//div[h3[.='Garage']]/p/text()").getall())
        if parking:
            if "0" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        square_meters = response.xpath("//*[@class='details']//tr[contains(.,'Surface area')]/td[2]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        else:
            square_meters = response.xpath("//*[@class='details']//tr[contains(.,'Land')]/td[2]//text()").get()
            if square_meters:
                item_loader.add_value("square_meters",square_meters.split("m")[0])
        address = ", ".join(response.xpath("//h1//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_value("title", re.sub("\s{2,}", " ", address))
        item_loader.add_xpath("city","//span[@class='city']/text()")
        landlord_name = response.xpath(".//div[@id='contact']//p[@class='name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name.strip())
            item_loader.add_xpath("landlord_phone", ".//div[@id='contact']//p[@class='phone']//text()")
            item_loader.add_xpath("landlord_email", ".//div[@id='contact']//p[@class='mail']//text()")
 
        yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "external_id": "Reference",
            # "square_meters": "Surface area",
            "floor": "Niveau",
            "utilities": "Charges",
            "bathroom_count": "Bathrooms",
            "room_count": "Bedrooms",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//div[h3[.='{v}']]/p/text()")

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
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
            "elevator": ["ascenseur", "ascenceur"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)