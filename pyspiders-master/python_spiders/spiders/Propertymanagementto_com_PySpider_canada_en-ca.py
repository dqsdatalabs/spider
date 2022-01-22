import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import math

class propertymanagementto_PySpider_canadaSpider(scrapy.Spider):
    name = 'propertymanagementto_com'
    allowed_domains = ['propertymanagementto.com']
    page_number = 2
    start_urls = [
        'https://www.propertymanagementto.com/available-properties/?_page=1'
        ]
    country = 'canada'
    locale = 'en-ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):  #page_follower
        urls = response.css("#pt-cv-view-2c88f396wp > div > div > div > a::attr(href)").extract()
        rent = response.css("#pt-cv-view-2c88f396wp > div > div > div > div.pt-cv-ctf-list > div:nth-child(1) > div > div::text").extract()
        room_count = response.css("#pt-cv-view-2c88f396wp > div > div > div > div.pt-cv-ctf-list > div:nth-child(2) > div > div::text").extract()
        bathroom_count = response.css("#pt-cv-view-2c88f396wp > div > div > div > div.pt-cv-ctf-list > div:nth-child(3) > div > div::text").extract()
        parking = response.css("#pt-cv-view-2c88f396wp > div > div > div > div.pt-cv-ctf-list > div:nth-child(4) > div > div::text").extract()
        for i in range(len(urls)):
            yield Request(url=urls[i],
            callback = self.parse_property,
            meta={'rent': rent,
                  'room_count':room_count,
                  'bathroom_count': bathroom_count,
                  'parking': parking})
        next_page = ("https://www.propertymanagementto.com/available-properties/?_page="+ str(propertymanagementto_PySpider_canadaSpider.page_number))
        if propertymanagementto_PySpider_canadaSpider.page_number <= 4:
            propertymanagementto_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)


    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("div:nth-child(1) > div:nth-child(3) > div.col-xs-12.col-sm-8.col-md-8.col-lg-8.property-border.prop-details > h1::text").get()
        rent= response.meta.get("rent")
        rent = int(rent[-1])
        room_count= response.meta.get("room_count")
        room_count = int(room_count[0].replace('.5',''))
        bathroom_count= response.meta.get("bathroom_count")
        parking= response.meta.get("parking")
        if parking[0] == '0':
            parking = False
        else:
            parking = True
        currency = "CAD"
        description = response.css("div:nth-child(1) > div.row.equal-height-prop-table > div.col.col-sm-12.col-md-8.col-lg-8.property-border").get().lower().split("2021")[1].split("available")[0]
        property_type = response.css("div:nth-child(1) > div.row.equal-height-prop-table > div.col.col-sm-12.col-md-8.col-lg-8.property-border > div.property-type > div > span > span > strong::text").get()
        if "Condominium" in property_type or "Apartment" in property_type:
            property_type = 'apartment'
        else:
            property_type = 'house'
        furnished = response.css("div:nth-child(1) > div.row.equal-height-prop-table > div.col.col-sm-12.col-md-8.col-lg-8.property-border > div.property-type > div > span > span::text").get()
        if "Unfurnished" in furnished:
            furnished = False
        else:
            furnished = True
        dishwasher = None
        washing_machine = None
        swimming_pool = None
        balcony = None
        if "dishwasher" in description:
            dishwasher = True
        if "washer" in description:
            washing_machine = True
        if "pool" in description:
            swimming_pool = True
        if "balcony" in description:
            balcony = True
        images_all= response.css("img::attr(src)").extract()
        images = ["0"]*100
        for i in range(len(images_all)):
            if 'jpg' in images_all[i] or 'png' in images_all[i] or 'jpeg' in images_all[i]:
                images[i] = images_all[i]
        j=0
        while "0" in images:
            if images[j] == "0":
                images.pop(j)
            else:
                j=j+1
        images.pop(0)
        external_images_count = len(images)

        city = 'Tornto'
        address = title+', '+city

        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('city',city)
        item_loader.add_value('address',address)
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)

        item_loader.add_value('rent',rent)
        item_loader.add_value('currency',currency)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('parking',parking)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('swimming_pool',swimming_pool)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name','propertymanagementto')
        item_loader.add_value('landlord_email','info@propertymanagementto.com')
        item_loader.add_value('landlord_phone','416 451 9499')
        yield item_loader.load_item()
