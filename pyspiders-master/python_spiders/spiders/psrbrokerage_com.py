import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class PsrbrokerageComSpider(scrapy.Spider):
    name = 'psrbrokerage_com'
    allowed_domains = ['psrbrokerage.com']
    start_urls = [
        'https://www.psrbrokerage.com/listings/?rental=yes&minbed=2&minbath=1&ps=1#038;minbed=2&minbath=1&ps=1']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#psr-listings-grid > div > div:nth-child(2) > div.fv-property-list > div"):
            url = appartment.css("a.psr-listing-card-link").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

        # try:
        #     next_page = response.css('a.next.page-numbers').attrib['href']
        #     next_page = "https://www.psrbrokerage.com/"+next_page
        # except:
        #     next_page = None

        # if next_page is not None:
        #     yield response.follow(next_page, callback=self.parse)

     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath(
            '//*[@id="psr-listings-item-container"]/div/div[2]/div[1]/section[1]/div/div[2]/span/span[3]/text()').get()

        status = response.css(
            '#psr-listings-item-container > div > div:nth-child(2) > div.col-md-8 > section.psr-listings-item-meta > div > div.col-sm-5 > span > span:nth-child(2)::text').get()
        if "Commercial" in status:
            return

        title = response.css('#title > h2::text').get()
        city = title.split(' - ')[0].split(',')[1]

        rent = response.css(
            '#psr-listings-item-container > div > div:nth-child(2) > div.col-md-8 > section.psr-listings-item-meta > div > div.col-sm-3 > span::text').get().split("$")[1]

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        if "." in rent:
            rent = rent.split(".")[0]

        description = response.css(
            "#psr-listings-item-container > div > div:nth-child(2) > div.col-md-8 > section.psr-listings-item-description > div::text").get()

        images = response.css(
            '#slider > ul.slides > li > img::attr(src)').extract()

        space = response.css(
            '#psr-listings-item-container > div > div:nth-child(2) > div.col-md-8 > section.psr-listings-item-meta > div > div.col-sm-4 > span > span.sqft > strong::text').get()

        try:
            space = int(int(space.split('-')[0].strip()) * 0.0929)
        except:
            try:
                space = int(int(space.split('-')[-1].strip()) * 0.0929)
            except:
                pass

        features = response.css('#tags > li')[1:]

        bedrooms = None
        bathrooms = None
        parking = None
        for item in features:
            if "Bedrooms" in item.css('strong.tagname::text').get():
                bedrooms = item.css('li::text').extract()[1]
            elif "Bathrooms" in item.css('strong.tagname::text').get():
                bathrooms = item.css('li::text').extract()[1]
            elif "Parking Spaces" in item.css('strong.tagname::text').get():
                parking = item.css('li::text').extract()[1]
                try:
                    if int(parking) > 0:
                        parking = True
                except:
                    pass

        property_type = response.xpath(
            '//*[@id="psr-listings-item-container"]/div/div[2]/div[1]/section[1]/div/div[2]/span/span[2]/text()').get()

        prop_type = None
        if 'condo' in property_type.lower():
            prop_type = 'apartment'
        elif "house" in property_type.lower():
            prop_type = 'house'

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id.split(':')[-1])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", prop_type)
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", title)
        item_loader.add_value("city", city)
        item_loader.add_value("square_meters", int(int(space)*10.764))
        item_loader.add_value("parking", parking)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", "416 360 0688")
        item_loader.add_value("landlord_email", "info@psrbrokerage.com")
        item_loader.add_value("landlord_name", "PSR Brokerage.")

        yield item_loader.load_item()
