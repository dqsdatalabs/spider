import scrapy
from scrapy import Request
from ..loaders import ListingLoader


class targetimmobiliare_PySpider_italySpider(scrapy.Spider):
    name = 'targetimmobiliare'
    allowed_domains = ['targetimmobiliare.com']
    start_urls = ['http://www.targetimmobiliare.com/annunci-immobiliari/all-tipologia/all-zona/Affitto/Residenziale/all-citta/all-prezzo-minimo-euro/all-prezzo-massimo-euro/all-riferimento-annuncio/dataannuncio.html']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for item in response.css('div.property'):
            urls = response.css("div.property-details > a::attr(href)").extract()
        for url in urls:
            url="http://www.targetimmobiliare.com/"+ url
            yield Request(url=url,
                          callback=self.parse_property)

    def parse_property(self, response):
            item_loader = ListingLoader(response=response)
            title = response.css("#content > div > div > div.span9 > div > h1::text").get()
            discription = response.css("h3+ p::text").get().strip()
            city = response.css("#content > div > div > div.span9 > div > div > p:nth-child(3) > span:nth-child(1)::text").get().split(' : ')[-1]
            area1 = response.css("#content > div > div > div.span9 > div > div > p:nth-child(3) > span:nth-child(2)::text").get().split(' : ')[-1]
            area = area1+", "+city
            price = response.css("#content > div > div > div.span9 > div > div > p:nth-child(3) > span:nth-child(3)::text").get().split(' € ')[-1].strip()
            property_type1 = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(1)::text").get().lower().strip()
            if property_type1 == "appartamento":
                property_type = 'apartment'
            external_id = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(4)::text").get().strip()
            square_meters = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(6)::text").get().strip().split(' ')[0]
            room_count = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(8)::text").get().strip()
            bathroom_count = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(9)::text").get().strip()
            images = response.css('#rg-gallery > div.rg-thumbs > div > div.es-carousel > ul > li > a > img::attr(src)').extract()
            for i in range(len(images)):
                images[i] = "http://www.targetimmobiliare.com/" + images[i]
            external_images_count = len(images)

            utilities = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(24)::text").get().strip().split(' € ')[0]
            if utilities == "":
                utilities = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(25)::text").get().strip().split(' € ')[0]
            furniture = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(17)::text").get().strip()
            if "Completo" in furniture:
                furnished = True
            else:
                furnished = False
            floor = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(13)::text").get().strip()
            elevators = response.css("#content > div > div > div.span9 > div > div > div.row-fluid > div:nth-child(20)").get().strip()
            if "Si" in elevators:
                elevator = True
            else:
                elevator = False

            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value('title', title)
            item_loader.add_value('description', discription)
            item_loader.add_value('city', city)
            item_loader.add_value('address', area)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)
            item_loader.add_value('images', images)
            item_loader.add_value('external_images_count', external_images_count)
            item_loader.add_value('rent', price)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('utilities', utilities)
            item_loader.add_value('furnished', furnished)
            item_loader.add_value('floor', floor)
            item_loader.add_value('elevator', elevator)
            item_loader.add_value("landlord_phone", "+39 055 3841931")
            item_loader.add_value("landlord_email", "info@targetimmobiliare.com")
            item_loader.add_value("landlord_name", "targetimmobiliare")

            yield item_loader.load_item()
